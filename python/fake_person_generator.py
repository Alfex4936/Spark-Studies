from faker import Faker


fake = Faker()

N = 5000


with open("data/fake_person.txt", "w") as file:
    for i in range(N):
        name = fake.name()  # random name
        age = fake.random_int(18, 55)  # how old
        friends = fake.random_int(0, 500)  # how many friends this person has
        file.write(f"{i},{name},{age},{friends}\n")
