"""纯标准库测试运行器：python -m tests.run_all"""
import sys
import traceback

from tests import harness  # noqa: F401  先安装 pymysql/redis/fastapi stub
from tests import test_batches, test_pivot

MODULES = (test_pivot, test_batches)


def main():
    failures = 0
    total = 0
    for module in MODULES:
        for name in sorted(dir(module)):
            if not name.startswith('test_'):
                continue
            total += 1
            try:
                getattr(module, name)()
            except Exception:
                failures += 1
                print(f'FAIL  {module.__name__}.{name}')
                traceback.print_exc()
            else:
                print(f'ok    {module.__name__}.{name}')
    print(f'\n{total - failures}/{total} 通过')
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
