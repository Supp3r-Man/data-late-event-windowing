import argparse

from .report import report


def main(argv=None):
    ap = argparse.ArgumentParser(prog="latewin", description="Late-event windowing demo")
    ap.add_argument("cmd", choices=["demo"])
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--n", type=int, default=1500)
    a = ap.parse_args(argv)
    print(report(a.seeds, a.n))


if __name__ == "__main__":
    main()
