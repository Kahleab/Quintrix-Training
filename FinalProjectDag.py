from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import random


states = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}

gender = ['M', 'F']

quote_types = ['Liability', 'Collision', 'Comprehensive', 'Gap Insurance', 'Medical Payments Coverage',
              'Classic Car Insurance']

existing_customer = ['Y', 'N']

sales_channels = ['Agent', 'Call Center', 'Web']

vehicle_classes = {'small': ['Convertible', 'Coupe', 'Sedan', 'Hatchback'],
                 'medium': ['SUV/Crossover', 'Wagon'],
                 'large': ['Truck', 'Van/minivan']
                 }

offer_applied = ['Y', 'N']

table_columns = ['quote_id', 'first_name', 'last_name', 'state', 'gender', 'zip_cd', 'quote_type', 'existing_customer',
                 'sales_channel', 'vehicle_class', 'vehicle_size', 'offer_applied', 'quote_amount', 'dataset_date']

start_date = '2023-02-26 00:00:00'
start_date_obj = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG('load_random_data_into_table',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          catchup=False)

# Define the function to generate random data
def generate_random_data():
    current_timestamp = start_date_obj + timedelta(days=i)
    current_date = start_date_obj.date() + timedelta(days=i)
    chance_of_outlier = .03
    data = [( 
        if (random.random() < chance_of_outlier):
            range_num = random.randint(15000,25000)
        else:
            range_num = random.randint(4000,6000)
        for j in range(range_num):
            quote_id = f"{random.randint(100000, 999999)}_{current_date}"
            snapshot_time = current_timestamp + timedelta(hours=random.randrange(23),minutes=random.randrange(59), seconds=random.randrange(59))
            sex = random.choice(gender)
            first_name = names.get_first_name(gender='male' if sex == 'M' else 'female')
            last_name = names.get_last_name()
            state = random.choice(list(states.values()))
            zip_cd = random.randint(10000, 99999)
            quote_type = random.choice(quote_types)
            existing_cust = random.choice(random.choices(existing_customer, weights=(20, 75), k=2))
            sales_channel = random.choice(sales_channels)
            vehicle_size = random.choice(list(vehicle_classes.keys()))
            vehicle_class = random.choice(vehicle_classes[vehicle_size])
            apply_offer = random.choice(offer_applied)
            quote_amount = round(random.uniform(50.11, 999.99), 2))(for i in range(90))]

    return data



# Define the function to load data into the existing table
def load_data_into_table():
    conn = airflow.connect(host=<HOSTNAME>, dbname=<DBNAME>, user=<USERNAME>, password=<PASSWORD>)
    cur = conn.cursor()
    data = generate_random_data()
    for row in data:
        cur.execute(f"INSERT INTO caspian_src.quotes (quote_id, snapshot_time,first_name, last_name, state, gender, zip_cd, quote_id, existing_customer,sales_channel,vehicle_class,vehicle_size,offer_applied,quote_amount) VALUES ({row[0]}, {row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},{row[7]},{row[8]},{row[9]},{row[10]},{row[11]},{row[12]},{row[13]},{row[14]})")
    conn.commit()
    cur.close()
    conn.close()

# Define the tasks
generate_data_task = PythonOperator(
    task_id='generate_data',
    python_callable=generate_random_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_into_table,
    dag=dag,
)

# Set the order of the tasks
generate_data_task >> load_data_task
