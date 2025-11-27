"""
Performance Monitoring Utilities for Analytics Endpoints
========================================================
Decorators and utilities to track response times, query performance,
and cache hit rates for Buffer-style analytics implementation.

Usage in app.py:
    from utils.performance_monitor import monitor_performance, log_cache_hit
    
    @app.route('/api/analytics-summary')
    @monitor_performance('analytics_summary')
    def analytics_summary():
        cached_data = get_cached_summary()
        log_cache_hit('analytics_summary', cached_data is not None)
        return jsonify(cached_data)
"""

import logging
import time
import functools
from datetime import datetime
from collections import defaultdict

# Configure logger
logger = logging.getLogger('performance_monitor')
logger.setLevel(logging.INFO)

# Performance statistics storage (in-memory for now, can be moved to Redis/DB)
performance_stats = defaultdict(lambda: {
    'total_calls': 0,
    'total_time': 0,
    'min_time': float('inf'),
    'max_time': 0,
    'avg_time': 0,
    'cache_hits': 0,
    'cache_misses': 0,
    'errors': 0
})


def monitor_performance(endpoint_name):
    """
    Decorator to monitor endpoint performance.
    Tracks response time and logs slow queries.
    
    Args:
        endpoint_name: Name identifier for the endpoint
    
    Example:
        @monitor_performance('analyze_page')
        def analyze():
            return render_template('analyze.html')
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            error_occurred = False
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                error_occurred = True
                performance_stats[endpoint_name]['errors'] += 1
                logger.error(f"❌ {endpoint_name}: Error - {str(e)}")
                raise
            finally:
                elapsed_time = time.time() - start_time
                elapsed_ms = elapsed_time * 1000
                
                # Update statistics
                stats = performance_stats[endpoint_name]
                stats['total_calls'] += 1
                stats['total_time'] += elapsed_time
                stats['min_time'] = min(stats['min_time'], elapsed_time)
                stats['max_time'] = max(stats['max_time'], elapsed_time)
                stats['avg_time'] = stats['total_time'] / stats['total_calls']
                
                # Log performance
                status = '❌ ERROR' if error_occurred else '✅'
                
                if elapsed_ms < 100:
                    level = '🚀 FAST'
                    log_func = logger.info
                elif elapsed_ms < 1000:
                    level = '✓ GOOD'
                    log_func = logger.info
                elif elapsed_ms < 3000:
                    level = '⚠️  SLOW'
                    log_func = logger.warning
                else:
                    level = '🐌 VERY SLOW'
                    log_func = logger.warning
                
                log_func(
                    f"{status} {level} | {endpoint_name} | "
                    f"{elapsed_ms:.2f}ms | "
                    f"avg: {stats['avg_time']*1000:.2f}ms | "
                    f"calls: {stats['total_calls']}"
                )
        
        return wrapper
    return decorator


def log_cache_hit(endpoint_name, is_hit):
    """
    Log cache hit/miss for analytics endpoints.
    
    Args:
        endpoint_name: Name identifier for the endpoint
        is_hit: True if cache hit, False if cache miss
    """
    stats = performance_stats[endpoint_name]
    
    if is_hit:
        stats['cache_hits'] += 1
        logger.info(f"💾 CACHE HIT | {endpoint_name} | hit_rate: {get_cache_hit_rate(endpoint_name):.1f}%")
    else:
        stats['cache_misses'] += 1
        logger.info(f"💥 CACHE MISS | {endpoint_name} | hit_rate: {get_cache_hit_rate(endpoint_name):.1f}%")


def get_cache_hit_rate(endpoint_name):
    """
    Calculate cache hit rate percentage for an endpoint.
    
    Args:
        endpoint_name: Name identifier for the endpoint
    
    Returns:
        float: Cache hit rate percentage (0-100)
    """
    stats = performance_stats[endpoint_name]
    total = stats['cache_hits'] + stats['cache_misses']
    
    if total == 0:
        return 0.0
    
    return (stats['cache_hits'] / total) * 100


def log_query_performance(query_name, query_result_count, elapsed_time):
    """
    Log database query performance.
    
    Args:
        query_name: Descriptive name for the query
        query_result_count: Number of rows returned
        elapsed_time: Time taken in seconds
    """
    elapsed_ms = elapsed_time * 1000
    
    if elapsed_ms < 50:
        level = '🚀 FAST'
        log_func = logger.info
    elif elapsed_ms < 500:
        level = '✓ GOOD'
        log_func = logger.info
    elif elapsed_ms < 2000:
        level = '⚠️  SLOW'
        log_func = logger.warning
    else:
        level = '🐌 VERY SLOW'
        log_func = logger.warning
    
    log_func(
        f"🗄️  {level} QUERY | {query_name} | "
        f"{elapsed_ms:.2f}ms | "
        f"rows: {query_result_count}"
    )


def get_performance_summary(endpoint_name=None):
    """
    Get performance summary for monitoring dashboard.
    
    Args:
        endpoint_name: Optional specific endpoint, or None for all
    
    Returns:
        dict: Performance statistics
    """
    if endpoint_name:
        if endpoint_name not in performance_stats:
            return None
        
        stats = performance_stats[endpoint_name]
        return {
            'endpoint': endpoint_name,
            'total_calls': stats['total_calls'],
            'avg_response_time_ms': stats['avg_time'] * 1000,
            'min_response_time_ms': stats['min_time'] * 1000,
            'max_response_time_ms': stats['max_time'] * 1000,
            'cache_hit_rate': get_cache_hit_rate(endpoint_name),
            'error_rate': (stats['errors'] / stats['total_calls'] * 100) if stats['total_calls'] > 0 else 0
        }
    else:
        # Return summary for all endpoints
        summary = {}
        for endpoint, stats in performance_stats.items():
            summary[endpoint] = {
                'total_calls': stats['total_calls'],
                'avg_response_time_ms': stats['avg_time'] * 1000,
                'cache_hit_rate': get_cache_hit_rate(endpoint),
                'error_rate': (stats['errors'] / stats['total_calls'] * 100) if stats['total_calls'] > 0 else 0
            }
        return summary


def print_performance_report():
    """Print formatted performance report to console"""
    print("\n" + "="*80)
    print("PERFORMANCE MONITORING REPORT")
    print("="*80)
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    if not performance_stats:
        print("No performance data collected yet.")
        print("="*80 + "\n")
        return
    
    for endpoint, stats in performance_stats.items():
        print(f"\n{endpoint.upper()}")
        print("-" * 80)
        print(f"  Total Calls:     {stats['total_calls']}")
        print(f"  Avg Response:    {stats['avg_time']*1000:.2f} ms")
        print(f"  Min Response:    {stats['min_time']*1000:.2f} ms")
        print(f"  Max Response:    {stats['max_time']*1000:.2f} ms")
        
        total_cache_requests = stats['cache_hits'] + stats['cache_misses']
        if total_cache_requests > 0:
            hit_rate = get_cache_hit_rate(endpoint)
            print(f"  Cache Hit Rate:  {hit_rate:.1f}% ({stats['cache_hits']}/{total_cache_requests})")
        
        if stats['errors'] > 0:
            error_rate = (stats['errors'] / stats['total_calls'] * 100)
            print(f"  Errors:          {stats['errors']} ({error_rate:.1f}%)")
    
    print("\n" + "="*80 + "\n")


def reset_performance_stats(endpoint_name=None):
    """
    Reset performance statistics.
    
    Args:
        endpoint_name: Optional specific endpoint to reset, or None for all
    """
    if endpoint_name:
        if endpoint_name in performance_stats:
            del performance_stats[endpoint_name]
            logger.info(f"Reset performance stats for: {endpoint_name}")
    else:
        performance_stats.clear()
        logger.info("Reset all performance stats")


# Context manager for tracking code block performance
class PerformanceTimer:
    """
    Context manager to time code blocks.
    
    Example:
        with PerformanceTimer('load_posts') as timer:
            posts = Post.query.all()
        print(f"Took {timer.elapsed_ms:.2f}ms")
    """
    def __init__(self, name):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.elapsed_time = None
        self.elapsed_ms = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time
        self.elapsed_ms = self.elapsed_time * 1000
        
        if exc_type is None:
            if self.elapsed_ms < 100:
                level = '🚀 FAST'
            elif self.elapsed_ms < 1000:
                level = '✓ GOOD'
            else:
                level = '⚠️  SLOW'
            
            logger.info(f"{level} | {self.name} | {self.elapsed_ms:.2f}ms")
        else:
            # Check if it's a database table missing error (expected before migration)
            exc_str = str(exc_val).lower()
            if 'does not exist' in exc_str or 'no such table' in exc_str:
                # Don't log as error - this is expected before migration runs
                pass
            else:
                # Log other errors normally
                logger.error(f"❌ ERROR | {self.name} | {exc_type.__name__}: {exc_val}")


if __name__ == '__main__':
    # Demo usage
    print("Performance Monitor Utilities")
    print("="*60)
    print("\nImport this module in your app.py:")
    print("  from utils.performance_monitor import monitor_performance, log_cache_hit")
    print("\nExample usage:")
    print("  @monitor_performance('analytics_summary')")
    print("  def analytics_summary():")
    print("      cached = get_cached_data()")
    print("      log_cache_hit('analytics_summary', cached is not None)")
    print("      return jsonify(cached)")
    print("="*60)
