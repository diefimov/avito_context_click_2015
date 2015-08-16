import argparse
import warnings
import os
import gzip
from csv import DictReader
from collections import namedtuple
from datetime import datetime

OutInfo = namedtuple('OutInfo', ['writer', 'out_file', 'selector', 'weight_builder', 'weight_writer'])


def convert_csv_2_libsvm(input_files, feat_map_file, out_selector,
                         col_out, col_in_cat, col_in_num, missing_values,
                         feat_start, weight_builder_dict):

    start = datetime.now()
    out_lst = [OutInfo(writer=open_write(out_file),
                       out_file=out_file,
                       selector=expr,
                       weight_builder=weight_builder_dict[out_file] if out_file in weight_builder_dict else None,
                       weight_writer=open_write(out_file + '.weight') if out_file in weight_builder_dict else None)
               for out_file, expr in out_selector.items()]
    missing_values = set(missing_values)

    feat_map = {}
    feat_map_list = ['skip' + str(ix) for ix in range(feat_start)]
    for col_in in col_in_cat:
        feat_map[col_in] = {}
    for col_in in col_in_num:
        feat_map[col_in] = len(feat_map_list)
        feat_map_list.append(col_in + '\tfloat')

    row_count = 0
    for input_file in input_files:
        for row in DictReader(open_read(input_file)):

            row_count += 1

            out_select = [out_stream.selector(input_file, row) for out_stream in out_lst]

            if sum(out_select) > 0:
                cur_row = row[col_out]
                if cur_row in missing_values:
                    cur_row = '0'

                for col_in in col_in_cat:
                    col_map = feat_map[col_in]
                    col_val = row[col_in]
                    if col_val not in missing_values:
                        if col_val in col_map:
                            col_feat = col_map[col_val]
                        else:
                            col_feat = col_map[col_val] = len(feat_map_list)
                            feat_map_list.append(col_in + '=' + col_val + '\ti')
                        cur_row += ' ' + str(col_feat) + ':1'

                for col_in in col_in_num:
                    col_val = row[col_in]
                    if col_val not in missing_values:
                        col_feat = feat_map[col_in]
                        cur_row += ' ' + str(col_feat) + ':' + col_val

                for i, out_stream in enumerate(out_lst):
                    if out_select[i]:
                        out_stream.writer.write(cur_row)
                        out_stream.writer.write('\n')
                        if out_stream.weight_builder is not None:
                            cur_weight = str(out_stream.weight_builder(input_file, row))
                            out_stream.weight_writer.write(cur_weight)
                            out_stream.weight_writer.write('\n')

            if row_count % 10000000 == 0:
                print('Lines read: %d, Elapsed time: %s' % (row_count, str(datetime.now() - start)))

    for out_stream in out_lst:
        out_stream.writer.close()
        if out_stream.weight_writer is not None:
            out_stream.weight_writer.close()

    if feat_map_file is not None:
        with open_write(feat_map_file) as fmap_out:
            for ix, fvalue in enumerate(feat_map_list):
                fmap_out.write(str(ix) + '\t' + fvalue + '\n')

    print('Total lines read: %d, Elapsed time: %s' % (row_count, str(datetime.now() - start)))


def open_read(path):

    if not os.path.exists(path) and os.path.exists(path + '.gz'):
        path += '.gz'
    print('\nOpening %s to read.' % path)

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

    # -input_files ../data/dmitry/data.all.tree.dl.csv.samp
    # -out_selector
    # "{'../data/dmitry/data.all.tree.dl.csv.samp.libsvm': lambda file, row: True}"
    # -feat_map_file ../data/dmitry/data.all.tree.dl.csv.samp.fmap
    # -col_out IsClick
    # -col_in_cat
    #   AdCatID
    # -col_in_num
    #   AdHistCTR AdPrice Position UserQryTotalTime

    parser = argparse.ArgumentParser(description='Conver a csv file to libffm format',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-input_files',
                        default=[],
                        type=str,
                        nargs='+',
                        help='CSV with training data')

    parser.add_argument('-feat_map_file',
                        default=None,
                        type=str,
                        help='Path to output feat map')

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

    parser.add_argument('-missing_values',
                        default=['', 'na', "nan", 'NA', 'NaN'],
                        type=str,
                        nargs='+',
                        help='List of the names of the numerical input columns')

    parser.add_argument('-feat_start',
                        default=0,
                        type=int,
                        help='Starting index of features')

    parser.add_argument('-weight_builder_dict',
                        default='{}',
                        type=str,
                        help='create weight features')

    args = vars(parser.parse_args())
    args['out_selector'] = eval(args['out_selector'])
    if args['weight_builder_dict'] is not None:
        args['weight_builder_dict'] = eval(args['weight_builder_dict'])

    print('\n' + (' '*300) + '\n')
    print(args)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=Warning)
        convert_csv_2_libsvm(**args)
