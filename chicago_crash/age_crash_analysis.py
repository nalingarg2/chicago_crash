def group_df(word):
    # print(int(word['AGE']), int(word['INJURIES_TOTAL']))
    # print(type(word['AGE']), type(word['INJURIES_TOTAL']))
    if int(word['AGE']) < 16:
        return (15, 1)
    elif int(word['AGE']) > 15 and int(word['AGE']) < 18:
        return (1617, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 17 and int(word['AGE']) < 20:
        return (1819, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 19 and int(word['AGE']) < 25:
        return (2024, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 24 and int(word['AGE']) < 30:
        return (2529, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 29 and int(word['AGE']) < 40:
        return (3039, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 39 and int(word['AGE']) < 50:
        return (4049, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 49 and int(word['AGE']) < 60:
        return (5059, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 59 and int(word['AGE']) < 70:
        return (6069, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 69 and int(word['AGE']) < 80:
        return (7079, int(word['INJURIES_TOTAL']))

    elif int(word['AGE']) > 79:
        return (80100, int(word['INJURIES_TOTAL']))