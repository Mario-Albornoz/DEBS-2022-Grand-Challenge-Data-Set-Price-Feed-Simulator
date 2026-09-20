#!/usr/bin/env python3
"""
Filter CSV files to only include trading hours (09:30-16:00).
Optimized version with progress bar.
"""

import sys
import time
from datetime import datetime
from pathlib import Path


def parse_time(time_str):
    """Parse time string from CSV (format: HH:MM:SS.mmm)"""
    try:
        # Just extract HH:MM for speed
        hour, minute = time_str.split(":")[:2]
        return int(hour), int(minute)
    except:
        return None, None


def is_trading_hours_fast(time_str):
    """Fast check if time is within trading hours (09:30-16:00)"""
    hour, minute = parse_time(time_str)
    if hour is None:
        return False

    # 09:30:00 to 16:00:00
    if hour < 9 or hour > 16:
        return False
    if hour == 9 and minute < 30:
        return False
    if hour == 16 and minute > 0:
        return False

    return True


def filter_file(input_path, output_path):
    """Filter a single CSV file to trading hours only"""

    file_size = input_path.stat().st_size
    file_size_mb = file_size / (1024 * 1024)

    print(f"\nProcessing {input_path.name} ({file_size_mb:.1f} MB)")

    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        header_lines = 0
        for line in infile:
            if line.startswith("#"):
                outfile.write(line)
                header_lines += 1
            else:
                outfile.write(line)
                header_lines += 1
                break

        kept = 0
        skipped = 0
        total_lines = 0
        bytes_processed = 0
        start_time = time.time()
        last_report = start_time

        estimated_lines = file_size / 80

        for line in infile:
            total_lines += 1
            bytes_processed += len(line)

            parts = line.strip().split(",")
            if len(parts) < 4:
                continue

            time_col = parts[3]  # Time column is 4th (index 3)

            if is_trading_hours_fast(time_col):
                outfile.write(line)
                kept += 1
            else:
                skipped += 1

            if total_lines % 500000 == 0:
                now = time.time()
                elapsed = now - start_time
                progress_pct = (bytes_processed / file_size) * 100
                lines_per_sec = total_lines / elapsed if elapsed > 0 else 0
                mb_per_sec = (
                    (bytes_processed / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                )

                if bytes_processed > 0:
                    eta_seconds = (
                        (file_size - bytes_processed) / bytes_processed
                    ) * elapsed
                    eta_min = int(eta_seconds / 60)
                    eta_sec = int(eta_seconds % 60)
                    eta_str = f"ETA: {eta_min}m {eta_sec}s"
                else:
                    eta_str = "ETA: calculating..."

                print(
                    f"  Progress: {progress_pct:.1f}% | "
                    f"{total_lines:,} lines | "
                    f"Kept: {kept:,} ({100*kept/(kept+skipped):.1f}%) | "
                    f"Speed: {lines_per_sec/1000:.0f}k lines/s, {mb_per_sec:.1f} MB/s | "
                    f"{eta_str}"
                )

                last_report = now

        elapsed = time.time() - start_time
        print(f"\n  ✓ Completed in {elapsed:.1f}s")
        print(f"    Total lines:  {total_lines:,}")
        print(f"    Kept:         {kept:,} ({100*kept/(kept+skipped):.1f}%)")
        print(f"    Skipped:      {skipped:,} ({100*skipped/(kept+skipped):.1f}%)")
        print(f"    Throughput:   {total_lines/elapsed/1000:.0f}k lines/s")

        return kept, skipped


def main():
    data_dir = Path(__file__).parent.parent / "data"
    output_dir = data_dir / "trading_hours"

    if output_dir.exists():
        print(f"Output directory already exists: {output_dir}")
        response = input("Delete and recreate? (y/n): ").lower()
        if response != "y":
            print("Aborted.")
            return
        import shutil

        shutil.rmtree(output_dir)

    output_dir.mkdir(exist_ok=True)

    csv_files = sorted(data_dir.glob("debs2022-gc-trading-day-*.csv"))

    if not csv_files:
        print(f" No CSV files found in {data_dir}")
        return

    print("=" * 70)
    print(f"Filtering {len(csv_files)} CSV files to trading hours (09:30-16:00)")
    print("=" * 70)

    total_kept = 0
    total_skipped = 0
    overall_start = time.time()

    for i, csv_file in enumerate(csv_files, 1):
        print(f"\n[{i}/{len(csv_files)}] {csv_file.name}")
        output_file = output_dir / csv_file.name

        try:
            kept, skipped = filter_file(csv_file, output_file)
            total_kept += kept
            total_skipped += skipped
        except KeyboardInterrupt:
            print("\n\n⚠ Interrupted by user (Ctrl+C)")
            print(f"Partial output saved to: {output_dir}")
            print("You can:")
            print("  1. Resume by re-running (will skip completed files)")
            print(f"  2. Delete partial output: rm -rf {output_dir}")
            sys.exit(1)
        except Exception as e:
            print(f"\n✗ Error processing {csv_file.name}: {e}")
            import traceback

            traceback.print_exc()
            continue

    overall_elapsed = time.time() - overall_start

    print("\n" + "=" * 70)
    print("✓ All files processed!")
    print("=" * 70)
    print(f"Total time:      {overall_elapsed/60:.1f} minutes")
    print(
        f"Total kept:      {total_kept:,} lines ({100*total_kept/(total_kept+total_skipped):.1f}%)"
    )
    print(
        f"Total skipped:   {total_skipped:,} lines ({100*total_skipped/(total_kept+total_skipped):.1f}%)"
    )
    print(f"Output location: {output_dir}")
    print()
    print("Next steps:")
    print(f"  1. Update simulator config: data_path: './data/trading_hours/'")
    print(
        f"  2. Or move files: mv {output_dir}/*.csv {data_dir}/ (after backing up originals)"
    )


if __name__ == "__main__":
    main()
