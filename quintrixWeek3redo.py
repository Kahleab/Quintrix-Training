import pandas as pd
import random
import datetime


def random_date(start : datetime, end : datetime):
    diff = datetime.timedelta(days=(end - start).days)
    rae = random.random() * diff
    return start + datetime.timedelta(days = rae.days)


def generate_dummy_info(record_count):
    print(record_count)
    ran_data = []
    start_date = datetime.datetime(2018, 5, 1)
    end_date = datetime.datetime(2020, 5, 1)

    for i in range(0, record_count):
        id_start = f"{i+1:07}"
        brand=random.choice(['Apple','Samsung','Nokia'])
        if brand=='Apple':
            product=random.choice(['iphone11','iphone12','iphone13','iphoneSE','IpadMax','IpadMini','laptop256','Macbook512'])
        elif brand=='Samsung':
            product=random.choice(['galaxy10','galaxy11','galaxy12','galaxy13','watch320','watch340'])
        else:
            product=random.choice(['Nk320','Nk400','Nk500'])
        sales_ammt = (random.randint(0, 9999999))/100
        sales_date = random_date(start_date,end_date)
        ran_data.append((id_start, brand, product, sales_ammt, sales_date))

    return ran_data


if __name__ == '__main__':
    record_num_request = 50000
    df = pd.DataFrame(generate_dummy_info(record_num_request), columns=['orderid', 'brand_name', 'product_name', 'sales_ammount', 'sales_date'])
    df.to_csv('C:/Users/kahle/OneDrive/Documents/order_data_20230401.csv', index= False)