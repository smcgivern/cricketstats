import argparse
import datetime
import glob
import numpy as np
import pandas as pd

def get_dataset(args):
    gender = 'women' if args.women else 'men'
    form = 'bowling' if args.bowling else 'batting'

    if args.teams:
        df = pd.read_pickle(f'data/{gender}_{args.format}_team.pkl')
    else:
        df = pd.read_pickle(f'data/{gender}_{args.format}_{form}.pkl')

    return df

def cumulative_average(data):
    data['cumulative_average'] = data['runs'].expanding().sum() / data['outs'].expanding().sum()

    return data

def main():
    parser = argparse.ArgumentParser(description='Highest lowest cumulative career average.')
    parser.add_argument('--women', '-w', default=False, action='store_true')
    parser.add_argument('--bowling', '-b', default=False, action='store_true')
    parser.add_argument('--teams', '-t', default=False, action='store_true')
    parser.add_argument('--format', '-f', default='test', choices=['test', 'odi', 't20i'], help='The format you want data for, default is test')
    parser.add_argument('--min-runs', '-r', default='1000')

    args = parser.parse_args()

    data = get_dataset(args) \
        .sort_values(by=['player', 'start_date', 'innings'])

    data['runs'] = data['runs'].astype(np.float64)
    data['outs'] = np.where(data['not_out'], 0, 1)
    data = data.groupby(['player']).apply(cumulative_average)

    results = data \
        .groupby(['player']) \
        .agg(runs=('runs', 'sum'), cumulative_average=('cumulative_average', 'min'))

    results = results \
        .query('runs > %s' % args.min_runs) \
        [np.isfinite(results)] \
        .sort_values(by='cumulative_average', ascending=[False]) \
        .head(20)

    print(results)

if __name__ == '__main__':
    main()
