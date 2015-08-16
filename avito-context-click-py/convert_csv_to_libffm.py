import argparse
import warnings
import os
import gzip
from csv import DictReader
from collections import namedtuple
from datetime import datetime
import sys

OutInfo = namedtuple('OutInfo', ['writer', 'selector'])


def convert_csv_2_libffm(input_files, out_selector, col_out, col_in_cat, col_in_num, old_format, silent):

    if old_format and len(col_in_num) > 0:
        raise ValueError('Old format doesn''t support numeric columns')

    start = datetime.now()
    out_lst = [OutInfo(writer=open_write(out_file), selector=expr) for out_file, expr in out_selector.items()]
    invalid_output = {'', 'na', "nan", 'NA', 'NaN'}

    feat_map = {}
    feat_index = 1
    for col_in in col_in_cat:
        feat_map[col_in] = {}
    for col_in in col_in_num:
        feat_map[col_in] = feat_index
        feat_index += 1

    row_count = 0
    for input_file in input_files:
        for row in DictReader(open_read(input_file)):

            row_count += 1

            out_select = [out_stream.selector(input_file, row) for out_stream in out_lst]

            if sum(out_select) > 0:
                cur_row = row[col_out]
                if cur_row in invalid_output:
                    cur_row = '0'

                col_index = 1

                for col_in in col_in_cat:
                    col_map = feat_map[col_in]
                    col_val = row[col_in]

                    if col_val in col_map:
                        col_feat = col_map[col_val]
                    else:
                        col_feat = col_map[col_val] = feat_index
                        feat_index += 1

                    if old_format:
                        cur_row += ' ' + str(col_feat)
                    else:
                        cur_row += ' ' + str(col_index) + ':' + str(col_feat) + ':1.0'

                    col_index += 1

                for col_in in col_in_num:
                    col_feat = feat_map[col_in]
                    cur_row += ' ' + str(col_index) + ':' + str(col_feat) + ':' + row[col_in]

                    col_index += 1

                for i, out_stream in enumerate(out_lst):
                    if out_select[i]:
                        out_stream.writer.write(cur_row)
                        out_stream.writer.write('\n')

            if not silent and row_count % 10000000 == 0:
                print('Lines read: %d, Elapsed time: %s' % (row_count, str(datetime.now() - start)))

    for out_stream in out_lst:
        out_stream.writer.close()

    if not silent:
        print('Total lines read: %d, Elapsed time: %s' % (row_count, str(datetime.now() - start)))


def open_read(path):

    if not os.path.exists(path) and os.path.exists(path + '.gz'):
        path += '.gz'
    print('\nOpening %s to read.' % path)

    if path == 'sys.stdin':
        return sys.stdin

    if path.endswith('.gz'):
        return gzip.open(path, mode='rt')
    else:
        return open(path, mode='rt')


def open_write(path):
    path_dir = os.path.dirname(path)
    if not os.path.exists(path_dir):
        os.makedirs(path_dir)

    return open(path, 'w')

if __name__ == "__main__":

    # -input_files ../data/output-r/data.all.lr.csv.gz
    # -out_selector
    # "{'../data/output-libffm/fm_01/data.val.tr.fm': lambda file, row: row['SearchType'] in ['hist', 'tr'],
    # '../data/output-libffm/fm_01/data.val.tt.fm': lambda file, row: row['SearchType'] in ['val'],
    # '../data/output-libffm/fm_01/data.test.tr.fm': lambda file, row: row['SearchType'] in ['hist', 'tr', 'val'],
    # '../data/output-libffm/fm_01/data.test.tt.fm': lambda file, row: row['SearchType'] in ['test']}"
    # -col_out IsClick
    # -col_in_cat
    #   AdCatID AdHistCTRBin AdID AdParams AdPriceBin AdTitleSZBin
    #   Position
    #   SearchAdCount SearchAdT1Count SearchAdT2Count SearchAdT3Count
    #       SearchCatID SearchLocID SearchParamsSZBin SearchQuerySZBin SearchRussian
    #   UserID UserIPID UserPrevQryDateBin UserQryTotalTimeBin

    parser = argparse.ArgumentParser(description='Conver a csv file to libffm format',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-input_files',
                        default=[],
                        type=str,
                        nargs='+',
                        help='CSV with training data')

    parser.add_argument('-out_selector',
                        required=True,
                        type=str,
                        help='Dictionary with format {"out_file" : lambda file, row: is_instance(file, row)}'
                             ' to select instances')

    parser.add_argument('-col_out',
                        required=True,
                        type=str,
                        help='Output column.')

    parser.add_argument('-col_in_cat',
                        default=[],
                        type=str,
                        nargs='+',
                        help='List of the names of the categorical input columns')

    parser.add_argument('-col_in_num',
                        default=[],
                        type=str,
                        nargs='+',
                        help='List of the names of the numerical input columns')

    parser.add_argument('-silent',
                        default=False,
                        action='store_true',
                        help='Don''t print execution information')

    parser.add_argument('-old_format',
                        default=False,
                        action='store_true',
                        help="Outputs files in the old format")

    args = vars(parser.parse_args())
    args['out_selector'] = eval(args['out_selector'])

    # if not sys.stdin.isatty():
    #     args['input_files'].append('sys.stdin')

    if not args['silent']:
        print('\n\n\n\n\n\n\n\n\n')
        print(args)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=Warning)
        convert_csv_2_libffm(**args)


