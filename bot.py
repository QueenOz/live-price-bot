# MINIMAL CHANGE FOR 24/7 OPERATION
# Just change this ONE line in your existing code:

async def main():
    """Enhanced main with task supervision and auto-restart"""
    global fetch_task, connection_task, watchdog_task, shutdown_requested
    restart_count = 0
    max_restarts = 999999  # ✅ CHANGED FROM 10 TO 999999 - THAT'S IT!
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    while restart_count < max_restarts and not shutdown_requested:
        # ... rest of your code stays EXACTLY the same
        try:
            print(f"🚀 Starting enhanced trading bot (restart #{restart_count})...")
            
            await log_error_with_deduplication(
                error_type="startup",
                severity="info",
                message=f"Trading bot started (restart #{restart_count})",
                function_name="main",
                active_symbols_count=0
            )
            
            # Start all core tasks
            fetch_task = asyncio.create_task(fetch_symbols_loop())
            connection_task = asyncio.create_task(maintain_connection())
            insert_task = asyncio.create_task(insert_prices_loop())
            watchdog_task = asyncio.create_task(watchdog())
            
            # Wait for any task to complete (which shouldn't happen unless shutdown)
            done, pending = await asyncio.wait(
                [fetch_task, connection_task, insert_task, watchdog_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            if shutdown_requested:
                break
            
            # If we get here, a core task died unexpectedly
            restart_count += 1
            for task in done:
                exception = task.exception()
                if exception:
                    await log_error_with_deduplication(
                        error_type="task_crash",
                        severity="fatal",
                        message=f"Core task crashed: {str(exception)}",
                        function_name="main",
                        stack_trace="".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                    )
            
            print(f"💥 Core task died, restarting entire bot (attempt {restart_count}/{max_restarts})...")
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            
            # Exponential backoff before restart
            if restart_count < max_restarts:
                await exponential_backoff(restart_count, base_delay=5, max_delay=120)
            
        except Exception as e:
            restart_count += 1
            await log_error_with_deduplication(
                error_type="main_crash",
                severity="fatal",
                message=f"Main function crashed: {str(e)}",
                function_name="main",
                stack_trace=traceback.format_exc()
            )
            
            if restart_count < max_restarts:
                print(f"💀 Main crashed, restarting in 10s... (attempt {restart_count}/{max_restarts})")
                await asyncio.sleep(10)
            else:
                print("💀 Max restarts reached, giving up!")
                raise
