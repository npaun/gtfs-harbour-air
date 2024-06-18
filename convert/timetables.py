import camelot
import sys
from dataclasses import dataclass, field
import datetime
import csv
from pathlib import Path
import types
import re
from typing import Any


RE_SPACE = re.compile(r'\s+')


class MismatchError(Exception):
    pass


@dataclass
class Placemarks:
    origin: str = ''
    dest: str = ''
    direction_id: int = -1 

    @classmethod
    def parse(cls, text, delim):
        origin, dest = text.split(delim)
        return cls(cls.canonicalize(origin), cls.canonicalize(dest))

    @staticmethod
    def canonicalize(text):
        return RE_SPACE.sub(' ', text.replace('-', ' '))

    def set_direction_wrt(self, other):
        if (other.origin, other.dest) == (self.origin, self.dest):
            self.direction_id = 0
        elif (other.origin, other.dest) == (self.dest, self.origin):
            self.direction_id = 1
        else:
            raise MismatchError()


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

@dataclass
class ParseState:
    expected_placemarks: Placemarks = Placemarks()
    flights: list[Flight] = field(default_factory=list)
    placemarks: Placemarks = Placemarks()
    last_row: list[Any] = field(default_factory=list) 

    def next_file(self):
        self.placemarks = Placemarks()

    def next_table(self):
        self.last_row = []


def parse_tables(pdf_path, state):
    state.next_file()
    tables = camelot.read_pdf(pdf_path, flavor='stream', pages='1-end')
    flights = []
    try:
        for table in tables:
            parse_table(table.df.values.tolist(), state)
    except MismatchError:
        pass


def parse_table(tbl, state):
    state.next_table()
    for row in tbl:
        nn_row = nonnull(row)
        if is_header(nn_row):
            state.placemarks = Placemarks.parse(state.last_row[0], ' to ')
            state.placemarks.set_direction_wrt(state.expected_placemarks)
        elif len(nn_row) == 1:
            pass
        else:
            rs_row = resplit(nn_row)
            state.flights.append(Flight(
                    placemarks=state.placemarks,
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
                ))
        state.last_row = nn_row


def is_header(row):
    return row[0] == 'Flight #'


def nonnull(row):
    return [v.strip() for v in row if v.strip()]


def resplit(row):
    return nonnull(sum((v.split(' ') for v in row), []))



with open(sys.argv[1]) as csv_fp:
    routes = list(csv.DictReader(csv_fp))

schedule_dir = Path(sys.argv[2])
state = ParseState()
for route in routes:
    route = types.SimpleNamespace(**route)
    state.expected_placemarks = Placemarks.parse(route.route_long_name, ' - ')
    candidate_scheds = list(schedule_dir.glob(f'{route.npaun_series_id}[-_ ]*.pdf'))
    
    for pdf_path in candidate_scheds:
        print('adding to', route.route_long_name, 'from', pdf_path.name)
        parse_tables(str(pdf_path), state)

print(state.flights)
