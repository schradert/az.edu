"""
Useful functions and objects for transformations
"""
from re import search
from pathlib import Path
from argparse import ArgumentParser
from numpy import NAN

from sas7bdat import SAS7BDAT
from functional import pseq

from datatypes import Num, NumCast, InputOutput

ZERO_SYMBOLS = ["*", "< 2%"]

def convert2defaults(val: str, cast: NumCast = int) -> Num:
    """ Handle conversions of symbols meant to obscure identity in results """
    return 0. if not val or val in ZERO_SYMBOLS else cast(val)

def percentInequality2FloatProportion(val: str) -> float:
    """ Extract percentage from cell elements and convert to decimal """
    try:
        num = float(val)
    except ValueError:
        match = search(r'(\d+\.?\d+)%', val)
        if not match:
            return NAN
        num = float(match.group()[1])
        if num > 1:
            num /= 100
    return num

def sas7bdat2csv(input_filepath: Path, output_filepath: Path):
    """ Convert sas7bdat files (from SAS) to .csv format """
    print(
        f'Saving contents of {input_filepath} to {output_filepath} ... ', end=''
    )
    with SAS7BDAT(input_filepath.as_posix()) as reader:
        reader.to_data_frame().to_csv(output_filepath, index=False)
    print('done')

def input2inout_tuple(input_filepath: Path, output_dir: Path) -> InputOutput:
    """ Conversion utility for input to input + output path """
    output_basename = input_filepath.with_suffix('.csv').name
    return InputOutput(input_filepath, output_dir.joinpath(output_basename))

if __name__ == "__main__":
    # Parse shell arguments
    parser = ArgumentParser(description="Convert .sas7bdat to .csv")
    parser.add_argument('in_', metavar='IN', help='Path to .sas7bdat file')
    parser.add_argument(
        '--out', dest='out', metavar='OUT', default='',
        help='Path to output .csv file'
    )
    args = parser.parse_args()

    # create initial path iterable of input files
    in_path = Path(args.in_)
    if in_path.is_dir():
        out_path = Path(args.out) if args.out else in_path
        if not out_path.is_dir():
            raise Exception("--out must specify a directory too")
        files = in_path.iterdir()
    elif in_path.is_file():
        out_path = Path(args.out or in_path.parent)
        files = [in_path]
    else:
        raise FileNotFoundError(f"Specified input file ({args.in_}) NOT found!")

    # extract all .sas7bdat files with no pre-existing output file to convert
    (
        pseq(*files)
            .filter(lambda fin: fin.suffix == '.sas7bdat')
            .map(lambda fin: input2inout_tuple(fin, out_path))
            .filter_not(lambda io: io.out.is_file())
            .for_each(lambda io: sas7bdat2csv(io.in_, io.out))
    )