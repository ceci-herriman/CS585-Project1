
import random 

# generate data for CircleNetPage

numOfUserEntries = 100
userNicknames = ["CeciHerriman1", "CharlesH123", "JimmyJim99", "Laurie", "KyraBrown30", "MountLandyn"]
userJobs = ["Software Engineer", "Data Scientist", "Senior Manager", "Director of Software", "Test Engineer"]
userHobbies = ["Sewing", "Hockey", "Reading", "Cooking", "Painting", "Skydiving", "Soccer", "Lifting", "Working out"]

with open("CircleNetPage.txt", "w") as f:
    for i in range (1, numOfUserEntries + 1):
        id = i

        userNickname = random.choice(userNicknames)
        userJob = random.choice(userJobs)
        userRegionCode = random.randint(1, 50)
        userHobby = random.choice(userHobbies)

        f.write(f'{id},{userNickname},{userJob},{userRegionCode},{userHobby}\n')


# generate data for Follows 

numOfFollowEntries = 40 #will be 20,000,000
followDescs = ["Best friends in college", "Best friends in high school", "Close family friend", "Professional celebrity interest"]

with open("Follows.txt", "w") as f:
    for i in range (1, numOfFollowEntries + 1):
        colRel = i
        id1 = random.randint(1, numOfUserEntries)
        id2 = random.randint(1, numOfUserEntries)

        while id2 == id1:
            id2 = random.randint(1, numOfUserEntries)

        date = random.randint(1, 1000001)
        desc = random.choice(followDescs)

        f.write(f'{colRel},{id1},{id2},{date},{desc}\n')

# generate data for AcivityLog

numOfActivities = 100 #will be 10,000,000
actionTypes = ["Just viewing the page", "Viewed page, left a note", "Viewed page, followed user", "Viewed page, liked and commented on post"]

with open("ActivityLog.txt", "w") as f:
    for i in range (1, numOfActivities + 1):
        id = i
        byWho = random.randint(1, numOfUserEntries)
        whatPage = random.randint(1, numOfUserEntries)

        while byWho == whatPage: 
            whatPage = random.randint(1, numOfUserEntries)

        actionType = random.choice(actionTypes)
        time = date = random.randint(1, 1000001)

        f.write(f'{id},{byWho},{whatPage},{actionType},{time}\n')