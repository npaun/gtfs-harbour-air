import camelot
import sys
from dataclasses import dataclass
import datetime
import csv
from pathlib import Path
import types
import re


@dataclass
class Placemarks:
    origin: str
    dest: str
    direction_id: int = -1 
    
@dataclass
class Flight:
    placemarks: Placemarks
    flight_no: int
    start_date: datetime.date
    end_date: datetime.date
    departure_time: datetime.time
    arrival_time: datetime.time
    monday: bool
    tuesday: bool
    wednesday: bool
    thursday: bool
    friday: bool
    saturday: bool
    sunday: bool

def parse_tables(pdf_path, expected_placemarks):
    tables = camelot.read_pdf(pdf_path, flavor='stream', pages='1-end')
    last_placemarks = None
    flights = []
    try:
        for table in tables:
            last_placemarks, flights_tbl = parse_table(table.df.values.tolist(), expected_placemarks, last_placemarks)
            flights.extend(flights_tbl)
            print('---')
    except MismatchError:
        pass
    return flights


def parse_table(tbl, expected_placemarks, last_placemarks):
    last_row = []
    placemarks = last_placemarks
    flights = []
    for row in tbl:
        nn_row = nonnull(row)
        if is_header(nn_row):
            origin, dest = nonnull(last_row)[0].split(' to ')
            placemarks = Placemarks(canonicalize_place(origin), canonicalize_place(dest))
            placemarks.direction_id = align_placemarks(expected_placemarks, placemarks)
        elif len(nn_row) == 1:
            pass
        else:
            rs_row = resplit(nn_row)
            flight = Flight(
                    placemarks=placemarks,
                    flight_no=int(rs_row[0]),
                    start_date=datetime.datetime.strptime(rs_row[1], '%d-%b-%y'),
                    end_date=datetime.datetime.strptime(rs_row[2], '%d-%b-%y'),
                    departure_time=datetime.datetime.strptime(f'{rs_row[3]} {rs_row[4]}', '%I:%M %p'),
                    arrival_time=datetime.datetime.strptime(f'{rs_row[5]} {rs_row[6]}', '%I:%M %p'),
                    monday=rs_row[7] == 'M',
                    tuesday=rs_row[8] == 'Tu',
                    wednesday=rs_row[9] == 'W',
                    thursday=rs_row[10] == 'Th',
                    friday=rs_row[11] == 'F',
                    saturday=rs_row[12] == 'Sa',
                    sunday=rs_row[13] == 'Su'
                )
            flights.append(flight)

        last_row = nn_row

    return placemarks, flights


def is_header(row):
    return row[0] == 'Flight #'


def nonnull(row):
    return [v.strip() for v in row if v.strip()]

def resplit(row):
    return nonnull(sum((v.split(' ') for v in row), []))

RE_SPACE = re.compile(r'\s+')

def canonicalize_place(place):
    return RE_SPACE.sub(' ', place.replace('-', ' '))


class MismatchError(Exception):
    pass


def align_placemarks(expected, actual):
    if (expected.origin, expected.dest) == (actual.origin, actual.dest):
        return 0
    elif (expected.origin, expected.dest) == (actual.dest, actual.origin):
        return 1
    else:
        raise MismatchError()

with open(sys.argv[1], 'r') as csv_fp:
    routes = list(csv.DictReader(csv_fp))

schedule_dir = Path(sys.argv[2])
for route in routes:
    route = types.SimpleNamespace(**route)
    origin, dest = route.route_long_name.split(' - ')
    expected_placemarks = Placemarks(canonicalize_place(origin), canonicalize_place(dest))
    #print(expected_placemarks, route.npaun_series_id)
    candidate_scheds = list(schedule_dir.glob(f'{route.npaun_series_id}[-_ ]*.pdf'))
    flights_all = []
    for pdf_path in candidate_scheds:
        flights_all.extend(parse_tables(str(pdf_path), expected_placemarks))

    print(flights_all)
