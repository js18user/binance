
from getcar import *

if __name__ == "__main__":

    try:

        car = Car('Bugatti Veyron', 'bugatti.png', '0.312', '2')
        print(car.car_type, car.brand, car.photo_file_name, car.carrying, car.passenger_seats_count, sep='\n')
        truck = Truck('Nissan', 'nissan.jpeg', '1.5', '3.92x2.09x1.87')
        print(truck.car_type, truck.brand, truck.photo_file_name, truck.body_length, sep='\n')
        print(truck.body_width, truck.body_height, sep='\n')
        spec_machine = SpecMachine('Komatsu-D355', 'd355.jpg', '93', 'pipelayer specs')
        spec_machine6 = SpecMachine('Komatsu-D356', 'd355.jpg', '93', 'pipelayer specs')
        spec_machine7 = SpecMachine('Komatsu-D357', 'd355.jpg', '93', 'pipelayer specs')
        spec_machine8 = SpecMachine('Komatsu-D358', 'd355.jpg', '93', 'pipelayer specs')
        print(spec_machine.car_type, spec_machine.brand, spec_machine.carrying, spec_machine.extra, sep='\n')
        print(spec_machine6.car_type, spec_machine6.brand, spec_machine6.carrying, spec_machine6.extra, sep='\n')
        print(spec_machine7.car_type, spec_machine7.brand, spec_machine7.carrying, spec_machine7.extra, sep='\n')
        print(spec_machine8.car_type, spec_machine8.brand, spec_machine8.carrying, spec_machine8.extra, sep='\n')
        print(spec_machine.photo_file_name, spec_machine.extra, sep='\n')
        print(spec_machine.get_photo_file_ext())

        cars = get_car_list('example_cars.csv')

        print(len(cars))
        print(type(cars))

        for car in cars:
            print(type(car))

        print(cars[0].car_type, cars[0].brand, cars[0].carrying, cars[0].photo_file_name, cars[0].passenger_seats_count)
        print(cars[1].car_type, cars[1].brand, cars[1].carrying, cars[1].photo_file_name, cars[1].body_whl)
        print(cars[2].car_type, cars[2].brand, cars[2].carrying, cars[2].photo_file_name, cars[2].passenger_seats_count)
        print(cars[1].get_body_volume())
        print(cars[3].car_type, cars[3].brand, cars[3].carrying, cars[3].photo_file_name, cars[3].extra)
        print(cars[4].car_type, cars[4].brand, cars[4].carrying, cars[4].photo_file_name, cars[4].extra)
        print(cars[5].car_type, cars[5].brand, cars[5].carrying, cars[5].photo_file_name, cars[5].extra)
        print(cars[6].car_type, cars[6].brand, cars[6].carrying, cars[6].photo_file_name, cars[6].extra)
        print(cars[2].car_type, cars[2].get_photo_file_ext())

    except Exception as err:
        print('ERR IN GET CAR LIST', err)

exit()
