package com.example.order.consumer;

import org.springframework.stereotype.Service;

import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Read side for the Cassandra status log.
 *
 * Lives in the consumer package because the Cassandra repository is
 * package-private. The controller calls this service.
 *
 * The query is built to match the table's partition key (user_id, bucket),
 * where bucket = userId + "-" + YYYYMM. Because we always supply the full
 * partition key, this is a single-partition read with no ALLOW FILTERING, and
 * rows come back in event_time DESC order (the table's clustering order).
 */
@Service
public class OrderHistoryService {

    private static final DateTimeFormatter MONTH_FMT = DateTimeFormatter.ofPattern("yyyyMM");

    private final OrderStatusLogRepository statusLogRepository;

    public OrderHistoryService(OrderStatusLogRepository statusLogRepository) {
        this.statusLogRepository = statusLogRepository;
    }

    /**
     * Return a user's status events for the given month (format yyyyMM, e.g.
     * "202606"). If month is null or blank, the current month is used.
     */
    public List<OrderStatusHistoryResponse> getHistory(String userId, String month) {
        String yyyymm = normalizeMonth(month);
        String bucket = userId + "-" + yyyymm;

        return statusLogRepository.findByUserIdAndBucket(userId, bucket)
                .stream()
                .map(OrderStatusHistoryResponse::from)
                .toList();
    }

    private String normalizeMonth(String month) {
        if (month == null || month.isBlank()) {
            return YearMonth.now().format(MONTH_FMT);
        }
        // validate the caller passed something like yyyyMM; throws if not
        YearMonth parsed = YearMonth.parse(month, MONTH_FMT);
        return parsed.format(MONTH_FMT);
    }
}
