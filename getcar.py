import csv
import os.path


class CarBase:

    csv_car_type = 0
    csv_brand = 1
    csv_passenger_seats_count = 2
    csv_photo_file_name = 3
    csv_body_whl = 4
    csv_carrying = 5
    csv_extra = 6

    def __init__(self, brand, photo_file_name, carrying):
        self.brand = brand
        self.photo_file_name = photo_file_name
        self.carrying = float(carrying)

    def get_photo_file_ext(self):

        ext = os.path.splitext(self.photo_file_name)[1]

        return ext


class Car(CarBase):

    car_type = 'car'

    def __init__(self, brand, photo_file_name, carrying, passenger_seats_count):
        super().__init__(brand, photo_file_name, carrying)
        self.passenger_seats_count = int(passenger_seats_count)

    @classmethod
    def instance(cls, row):
        return cls(
            row[cls.csv_brand],
            row[cls.csv_photo_file_name],
            row[cls.csv_carrying],
            row[cls.csv_passenger_seats_count],
        )


class Truck(CarBase):

    car_type = 'truck'

    def __init__(self, brand, photo_file_name, carrying, body_whl):
        super().__init__(brand, photo_file_name, carrying)
        self.body_whl = body_whl

        try:

            length, width, height = (float(c) for c in body_whl.split('x', 2))

        except ValueError:
            length, width, height = .0, .0, .0

        self.body_length = length
        self.body_width = width
        self.body_height = height

    def get_body_volume(self):

        return self.body_width * self.body_height * self.body_length

    @classmethod
    def instance(cls, row):
        return cls(
            row[cls.csv_brand],
            row[cls.csv_photo_file_name],
            row[cls.csv_carrying],
            row[cls.csv_body_whl],
        )


class SpecMachine(CarBase):

    car_type = 'spec_machine'

    def __init__(self, brand, photo_file_name, carrying, extra):
        super().__init__(brand, photo_file_name, carrying)
        self.extra = extra

    @classmethod
    def instance(cls, row):
        return cls(
            row[cls.csv_brand],
            row[cls.csv_photo_file_name],
            row[cls.csv_carrying],
            row[cls.csv_extra],
        )


def get_car_list(csv_filename):

    with open(csv_filename) as csv_fd:

        reader = csv.reader(csv_fd, delimiter=';')
        next(reader)
        car_list = []
        create_s = {car_class.car_type: car_class for car_class in (Car, Truck, SpecMachine)}

        for row in reader:

            try:

                ip0 = 0             # verify  csv_car_type 0
                ip1 = 0             # verify  csv_brand 1
                ip2 = 0             # verify  csv_passenger_seats_count 2
                ip3 = 0             # verify  csv_photo_file_name 3
                ip4 = 0             # verify  csv_body_whl 4
                ip5 = 0             # verify  csv_carrying 5
                ip6 = 0             # verify  csv_extra 6
                ip7 = 0             # verify  all elements in record 7
                ind = 0             # verify  com result for create
                car_type = ''

                if len(row) == 7:
                    ip7 = 1

                    try:

                        car_type = row[CarBase.csv_car_type]

                        if car_type in ('car', 'truck', 'spec_machine'):
                            ip0 = 1

                    except Exception as err:
                        print('ERROR in csv_car_type:  ', err)

                if (ip7 == 1) & (ip0 == 1):

                    if (row[CarBase.csv_brand]) != '':
                        ip1 = 1

                    if car_type == 'car':

                        try:

                            if (row[CarBase.csv_passenger_seats_count]) != '':

                                if isinstance(int(row[CarBase.csv_passenger_seats_count]), int):
                                    ip2 = 1

                        except Exception as err:
                            print('ERROR in csv_passenger_seats_count:  ', err)

                    try:

                        if (os.path.splitext(row[CarBase.csv_photo_file_name])[0]) != '':

                            if os.path.splitext(row[CarBase.csv_photo_file_name])[1] in ('.jpg', '.jpeg', '.png', '.gif'):
                                ip3 = 1

                    except Exception as err:
                        print('ERROR in csv_photo_file_name:   ', err)

                    if car_type == 'truck':
                        ip4 = 1

                        try:

                            if (row[CarBase.csv_body_whl]) != '':
                                l, w, h = (float(c) for c in row[CarBase.csv_body_whl].split('x', 2))

                            else:
                                row[CarBase.csv_body_whl] = '0.0x0.0x0.0'

                        except Exception as err:
                            print('ERROR in csv_body_whl:   ', err)

                    if car_type == 'spec_machine':

                        try:

                            if (row[CarBase.csv_extra]) != '':
                                ip6 = 1

                        except Exception as err:
                            print('ERROR in csv_extra:   ', err)

                    try:

                        if len(row[CarBase.csv_carrying]) != '':

                            if isinstance(float(row[CarBase.csv_carrying]), float):
                                ip5 = 1

                    except (ValueError, IndexError) as err:
                        print('ERROR in csv_carrying:   ', err)

                    if (car_type == 'car') & ((ip0 * ip1 * ip2 * ip3 * ip5 * ip7) == 1):
                        ind = 1

                    if (car_type == 'truck') & ((ip0 * ip1 * ip3 * ip4 * ip5 * ip7) == 1):
                        ind = 1

                    if (car_type == 'spec_machine') & ((ip0 * ip1 * ip3 * ip5 * ip6 * ip7) == 1):
                        ind = 1

                    if ind == 1:

                        try:

                            car_class = create_s[car_type]

                        except Exception as err:
                            print('ERROR in car_class:   ', err)

                        try:

                            car_list.append(car_class.instance(row))

                        except Exception as err:
                            print('ERROR in machine append:    ', err)

            except Exception as err:
                print('ERROR in read next record of svc.file:   ', err)

        csv_fd.close()

    return car_list
