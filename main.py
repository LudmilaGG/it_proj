import task1
import task2
import task3
import task4

task_choices = {
    1: ("1. Task 1", task1.main),
    2: ("2. Task 2", task2.main),
    3: ("3. Task 3", task3.main),
    4: ("4. Task 4", task4.main),
    0: ("0. Exit", exit),
}

print("Hello!")


while True:

    for choice in task_choices.values():
        print(choice[0])

    task_choice = input("Please choose task: ")
    while task_choice not in task_choices.keys():
        try:
            task_choice = int(task_choice)
        except ValueError:
            print("You entered wrong value! Please try again...")
            task_choice = input("Please choose task: ")

    choice_t = task_choices[task_choice]
    print("You've chosen %s" % choice_t[0])
    choice_t[1]()
