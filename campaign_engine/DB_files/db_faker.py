from faker import Faker
import random
import psycopg2

fake = Faker()

conn = psycopg2.connect(
    dbname="campaign_db",
    user="balkrishna",
    password="postgresql@14",
    host="localhost"
)

cur = conn.cursor()


"""
# ----------------------------------------------------- #
# Insert into customer_master                           #
# ----------------------------------------------------- #
"""

# for i in range(10):
#     cur.execute("""
#         INSERT INTO cdm.customer_master
#         (exclusion_id, 
#         first_name, 
#         last_name, 
#         date_of_birth, 
#         gender, 
#         email, 
#         phone, 
#         segment)
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
#     """, (
#         fake.uuid4(),
#         fake.first_name(),
#         fake.last_name(),
#         fake.date_of_birth(minimum_age=18, maximum_age=70),
#         random.choice(['M','F','O']),
#         fake.email(),
#         fake.phone_number()[:20],
#         random.choice(['Retail','HNI','Corporate'])[:20]
#     ))


"""
# ----------------------------------------------------- #
# Insert into seedlist_users                            #
# ----------------------------------------------------- #
"""


# for i in range(10):
#     staff_id = f"BKS{i:04d}"
#     cur.execute("""
#         INSERT INTO cdm.seedlist_users
#         (exclusion_id, 
#         first_name, 
#         last_name, 
#         date_of_birth, 
#         gender, 
#         email, 
#         phone, 
#         segment,
#         staffid)
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
#     """, (
#         fake.uuid4(),
#         fake.first_name(),
#         fake.last_name(),
#         fake.date_of_birth(minimum_age=18, maximum_age=70),
#         random.choice(['M','F','O']),
#         fake.email(),
#         fake.phone_number()[:20],
#         random.choice(['Retail','HNI','Corporate'])[:20],
#         staff_id
#     ))


"""
# ----------------------------------------------------- #
# Insert into customer_exclusion_master                 #
# ----------------------------------------------------- #
"""

cur.execute("SELECT customer_ref_no FROM customer_master_1")
customers = [r[0] for r in cur.fetchall()]

for cust in customers:
    r = random.random()

    if r < 0.05:
        exclusion_type = 'GLOBAL'
        reason = 'FRAUD'

    elif r < 0.15:
        exclusion_type = 'MARKETING'
        reason = 'DND'

    elif r < 0.30:
        exclusion_type = 'CHANNEL'
        reason = 'NO_EMAIL'

    else:
        continue

    cur.execute("""
        INSERT INTO customer_exclusion_master
        (customer_ref_no, exclusion_type, reason_code)
        VALUES (%s,%s,%s)
    """, (cust, exclusion_type, reason))

"""
# ----------------------------------------------------- #
# Insert into contact_policy_master                     #
# ----------------------------------------------------- #
"""


# sources = ["HomeLoan", "CreditCard", "TopUp"]
# rows = []
# for source in sources:
#     # selected = random.choices(customers, k=1000)  # allow repeat
#     rows.append((
#         source,
#         10,
#         10,
#         15))
# cur.executemany("""
# INSERT INTO cdm.contact_policy_master
# (source_type,sms_cap,email_cap,max_cap)
# VALUES (%s,%s,%s,%s)
# """, rows)
    

"""
# ----------------------------------------------------- #
# Insert into customer_contact_source                   #
# ----------------------------------------------------- #
"""

# sources = ["HomeLoan", "CreditCard", "TopUp"]

# cur.execute("SELECT customer_ref_no FROM cdm.customer_master")
# customers = [r[0] for r in cur.fetchall()]

# rows = []

# for source in sources:
    # selected = random.choices(customers, k=1000)  # allow repeat

    # for cust in customers:
    #     rows.append((
    #         cust,
    #         source,
    #         True,
    #         fake.email(),
    #         fake.phone_number()[:20]
            
    #     ))

# cur.executemany("""
# INSERT INTO cdm.customer_contact_source
# (customer_ref_no, source_type,is_active, email,phone)
# VALUES (%s,%s,%s,%s,%s)
# """, rows)


conn.commit()
# print("Inserted:", len(rows))

