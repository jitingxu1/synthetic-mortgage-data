import argparse
import pathlib
import os

from datetime import datetime
from multiprocessing import Pool
from uuid import uuid4

from generate_data import generate_loan_and_perf


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Mortgage data Generator')

    parser.add_argument('-start_year', type=int, help='loan acquisition start date', default=2000)
    parser.add_argument('-end_year', type=int, help='loan acquisition end date', default=2024)
    parser.add_argument('-sf', type=float, help='scale factor', default=0.01)
    parser.add_argument('-sf_name', type=str, help='scale factor', default=str(uuid4()))
    parser.add_argument('-max_mem_mb', type=int, help='max loans', default=10)
    parser.add_argument('-o', '--output_path', type=pathlib.Path, help='Output Folder path', default='data')
    parser.add_argument('-c', '--config_path', type=pathlib.Path, help='config file', default='./config')
    parser.add_argument('-pools', type=int, help='number pool', default=1)

    args = parser.parse_args()

    start_year = args.start_year
    end_year = args.end_year
    scale = args.sf
    sf_name = args.sf_name
    output_path = args.output_path
    config_path = args.config_path
    max_mem_mb = args.max_mem_mb
    pools = args.pools

    finished_list = ['1999Q4']

    mapped_args = [
        (f"{y}Q{q}", output_path, sf_name, config_path, max_mem_mb, scale) 
        for y in range(start_year, end_year + 1)
        for q in range(1,5)
        if f"{y}Q{q}" not in finished_list
    ]

    with Pool(pools) as p:
        p.starmap(generate_loan_and_perf, mapped_args)

