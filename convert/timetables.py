import camelot
import sys
from dataclasses import dataclass, field
import dataclasses
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

    @classmethod
    def parse(cls, text, delim):
        origin, dest = text.split(delim)
        return cls(cls.canonicalize(origin), cls.canonicalize(dest))

    @staticmethod
    def canonicalize(text):
        return RE_SPACE.sub(' ', text.replace('-', ' '))


@dataclass(frozen=True)
class Calendar:
    start_date: datetime.date
    end_date: datetime.date
    monday: bool
    tuesday: bool
    wednesday: bool
    thursday: bool
    friday: bool
    saturday: bool
    sunday: bool

    @staticmethod
    def gtfs_date(date_val):
        return date_val.strftime('%Y%m%d')

    @property
    def day_mask(self):
        days = [self.monday, self.tuesday, self.wednesday, self.thursday, self.friday, self.saturday, self.sunday]
        return ''.join(str(int(day)) for day in days)

    @property
    def service_id(self):
        return f'{Calendar.gtfs_date(self.start_date)}-{Calendar.gtfs_date(self.end_date)}-{self.day_mask}'


    @staticmethod
    def header():
        return ('service_id', 'start_date', 'end_date', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')

    def serialize(self):
        return (self.service_id, 
                Calendar.gtfs_date(self.start_date), 
                Calendar.gtfs_date(self.end_date),
                int(self.monday),
                int(self.tuesday),
                int(self.wednesday),
                int(self.thursday),
                int(self.friday),
                int(self.saturday),
                int(self.sunday)
                )


@dataclass
class StopTime:
    stop_id: str
    arrival_time: datetime.time
    departure_time: datetime.time
    trip_id: str = ''
    stop_sequence: int = -1

    @staticmethod
    def gtfs_time(time_val):
        return time_val.strftime('%H:%M:%S')

    @staticmethod
    def header():
        return ('trip_id', 'stop_sequence', 'stop_id', 'arrival_time', 'departure_time')

    def serialize(self):
        return (self.trip_id,
                self.stop_sequence,
                self.stop_id,
                StopTime.gtfs_time(self.arrival_time),
                StopTime.gtfs_time(self.departure_time)
                )


@dataclass
class Trip:
    route_id: str
    trip_short_name: int
    service: Calendar
    direction_id: int
    trip_headsign: str
    stop_times: list[StopTime] = field(default_factory=list)

    @property
    def trip_id(self):
        return f'YB{self.trip_short_name}-{self.service.service_id}'

    def add_stop_time(self, st):
        st.trip_id = self.trip_id
        st.stop_sequence = len(self.stop_times)
        self.stop_times.append(st)

    @staticmethod
    def header():
        return ('trip_id', 'route_id', 'service_id', 'direction_id', 'trip_headsign', 'trip_short_name')

    def serialize(self):
        return (self.trip_id,
                self.route_id,
                self.service.service_id,
                self.direction_id,
                self.trip_headsign,
                self.trip_short_name
                )


@dataclass
class ParseState:
    route: Any = None
    expected_placemarks: Placemarks = Placemarks()
    stops: list[str] = field(default_factory=list)
    flights: list[Trip] = field(default_factory=list)
    placemarks: Placemarks = Placemarks()
    direction_id: int = -1
    last_row: list[Any] = field(default_factory=list) 

    def next_file(self):
        self.placemarks = Placemarks()

    def next_table(self):
        self.last_row = []

    @staticmethod
    def get_direction_wrt(expected, actual):
        expected_tpl = dataclasses.astuple(expected)
        actual_tpl = dataclasses.astuple(actual)
        if expected_tpl == actual_tpl:
            return 0
        elif expected_tpl == tuple(reversed(actual_tpl)):
            return 1
        else:
            raise MismatchError()


def parse_route(schedule_dir, route, state):
    state.route = route
    state.expected_placemarks = Placemarks.parse(route.route_long_name, ' - ')
    state.stops = route.route_id.split('-')
    candidate_scheds = list(schedule_dir.glob(f'{route.npaun_series_id}[-_ ]*.pdf'))

    for pdf_path in candidate_scheds:
        print('adding to', route.route_long_name, 'from', pdf_path.name)
        parse_tables(str(pdf_path), state)


def parse_tables(pdf_path, state):
    state.next_file()
    tables = camelot.read_pdf(pdf_path, flavor='stream', pages='1-end')
    flights = []
    try:
        for i, table in enumerate(tables):
            print(f'\tTable {i}')
            parse_table(table.df.values.tolist(), state)
    except MismatchError:
        pass


def parse_table(tbl, state):
    state.next_table()
    for row in tbl:
        nn_row = nonnull(row)
        if is_header(nn_row):
            state.placemarks = Placemarks.parse(state.last_row[0], ' to ')
            state.direction_id = ParseState.get_direction_wrt(state.expected_placemarks, state.placemarks)
        elif len(nn_row) == 1:
            pass
        else:
            rs_row = resplit(nn_row)
            trip = Trip(
                    route_id=state.route.route_id,
                    trip_short_name=int(rs_row[0]),
                    direction_id=state.direction_id,
                    trip_headsign=state.placemarks.dest,
                    service=Calendar(
                        start_date=parse_date(rs_row[1]),
                        end_date=parse_date(rs_row[2]),
                        monday=rs_row[7] == 'M',
                        tuesday=rs_row[8] == 'Tu',
                        wednesday=rs_row[9] == 'W',
                        thursday=rs_row[10] == 'Th',
                        friday=rs_row[11] == 'F',
                        saturday=rs_row[12] == 'Sa',
                        sunday=rs_row[13] == 'Su'
                    )
            )
            
            departure_time = parse_time(rs_row[3], rs_row[4])
            arrival_time = parse_time(rs_row[5], rs_row[6])
            stops = state.stops if state.direction_id == 0 else list(reversed(state.stops))

            trip.add_stop_time(StopTime(
                stop_id=stops[0],
                arrival_time=departure_time,
                departure_time=departure_time,
            ))

            trip.add_stop_time(StopTime(
                stop_id=stops[1],
                arrival_time=arrival_time,
                departure_time=arrival_time
            ))

            print(f'\t\t{trip.serialize()}')

            state.flights.append(trip)

        state.last_row = nn_row


def parse_date(value):
    return datetime.datetime.strptime(value, '%d-%b-%y').date() 


def parse_time(time_val, ampm_val):
    return datetime.datetime.strptime(f'{time_val} {ampm_val}', '%I:%M %p').time()

def is_header(row):
    return row[0] == 'Flight #'


def nonnull(row):
    return [v.strip() for v in row if v.strip()]


def resplit(row):
    return nonnull(sum((v.split(' ') for v in row), []))


def csv_as_objects(csv_path):
    with open(csv_path, 'r', encoding='utf-8') as fp:
        for row_dict in csv.DictReader(fp):
            yield types.SimpleNamespace(**row_dict)


def write_all(csv_path, records):
    with open(csv_path, 'w', encoding='utf-8') as fp:
        wr = csv.writer(fp)
        wr.writerow(records[0].header())
        for row in records:
            wr.writerow(row.serialize())


def main():
    routes = csv_as_objects(sys.argv[1])
    schedule_dir = Path(sys.argv[2])
    out_dir = Path(sys.argv[3])

    state = ParseState()
    for route in routes:
        parse_route(schedule_dir, route, state)


    calendars = set()
    trips = {}
    for trip in state.flights:
        trips[trip.trip_id] = trip
        calendars.add(trip.service)

    write_all(out_dir / 'calendar.txt', list(calendars))
    write_all(out_dir / 'trips.txt', list(trips.values()))
    stop_times = sum((trip.stop_times for trip in trips.values()), [])
    write_all(out_dir / 'stop_times.txt', stop_times)


if __name__ == '__main__':
    main()
