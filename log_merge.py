import argparse
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from shutil import rmtree
from sys import exit
from typing import Generator


@dataclass
class LogRecord:
    log_level: str
    timestamp: str
    message: str
	
	@property
    def __timestamp(self):
        return datetime.strptime(self.timestamp, '%Y-%m-%d %H:%M:%S')

    def __lt__(self, other):
        if other is None:
            return True
        return self.__timestamp < other.__timestamp

    def __gt__(self, other):
        if other is None:
            return False
        return self.__timestamp > other.__timestamp


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Tool to merge two big log files.')
    parser.add_argument(
        'path_to_log_a',
        metavar='<PATH_TO_LOG_A>',
        type=str,
        help='path to dir with log_a file',
    )
    parser.add_argument(
        'path_to_log_b',
        metavar='<PATH_TO_LOG_B>',
        type=str,
        help='path to dir with log_b file',
    )
    parser.add_argument(
        '-o', '--output',
        dest='output_dir',
        metavar='<PATH_TO_MERGED_LOG>',
        required=True,
        type=str,
        help='path to dir with merged log',
    )
    parser.add_argument(
        '-w', '--overwrite',
        action='store_const',
        const=True,
        default=False,
        help='overwrite merged log file if exists',
        dest='overwrite',
    )
    return parser.parse_args()


def _check_path_to_log(log_path: Path) -> bool:
    if not (log_path.exists() and log_path.is_file()):
        raise FileNotFoundError('Wrong file path.')
    if log_path.suffix.lower() != '.jsonl':
        raise ValueError('Log file type is incorrect.')
    return True


def _create_output_dir(output_dir: Path, overwrite: bool = False) -> None:
    if output_dir.exists():
        if not overwrite:
            print(
                '''Output dir exists.'''
                ''' Enter any char to overwrite or 'No' to exit.'''
            )
            overwrite = input().lower() != 'no' or exit(0)
        if overwrite:
            rmtree(output_dir)
    output_dir.mkdir(parents=True)


def _get_record(log_path: Path) -> Generator[LogRecord]:
    if _check_path_to_log(log_path):
        with open(log_path) as log_file:
            for row in log_file:
                yield LogRecord(**json.loads(row))


def _get_output_record(log_a_path: Path, log_b_path: Path) -> Generator[LogRecord]:

    log_a_records = _get_record(log_a_path)
    log_b_records = _get_record(log_b_path)

    log_a_record = next(log_a_records)
    log_b_record = next(log_b_records)

    while log_a_record is not None or log_b_record is not None:
        if log_a_record < log_b_record:
            yield log_a_record
            try:
                log_a_record = next(log_a_records)
            except StopIteration:
                log_a_record = None
        else:
            yield log_b_record
            try:
                log_b_record = next(log_b_records)
            except StopIteration:
                log_b_record = None


def main() -> None:
    args = _parse_args()
    start_time = time.time()
    print('''merging logs...please wait...it's not fast :)''')

    output_dir = Path(args.output_dir)
    _create_output_dir(output_dir, overwrite=args.overwrite)
    output_file_path = output_dir.joinpath('merged_log.jsonl')
    with open(output_file_path, 'w', encoding='utf-8') as merged_log_file:
        output_records = _get_output_record(
            Path(args.path_to_log_a),
            Path(args.path_to_log_b)
        )
        for log_record in output_records:
            merged_log_file.write(json.dumps(asdict(log_record)) + '\n')
    print(f'finished in {time.time() - start_time:0f} sec.')


if __name__ == '__main__':
    main()
