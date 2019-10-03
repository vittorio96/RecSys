import random

def remove_duplicates(ordered_list):
    """
    :param ordered_list
    :return: the ordered_list still ordered removed of duplicates
    """

    seen = set()
    seen_add = seen.add
    return [x for x in ordered_list if not (x in seen or seen_add(x))]
    return list(set(ordered_list))


def remove_one_list_from_another(list, list_to_remove):
    return [x for x in list if x not in list_to_remove]


def add_element_to_rec_list(source_list, source_cursor, rec_list):

    while source_list[source_cursor] in rec_list and source_cursor < len(source_list):#find a missing element in rec_list
        source_cursor += 1

    rec_list.append(source_list[source_cursor])

    return source_cursor



def list_merger(tupleDict, nRec):
    """
        :param tupleDict : dict{(list_of_items, numAtEachRound)}
                            assumes list_of_items is ordered from best rec
                            to worst rec

        :param nRec      : number of items to recommend

        :return list of nRec items weighted according to dict
    """

    ii_items = tupleDict.get('ii')[0]
    ii_picked_items = 0

    slim_items = tupleDict.get('slim')[0]
    slim_picked_items = 0

    user_items = tupleDict.get('user')[0]
    user_picked_items = 0

    final_list = []
    nRounds = nRec

    # extraction of high priority items -> consensus
    max_priority_items = sorted(set(ii_items) & set(slim_items) & set(user_items), key = ii_items.index)

    medium_priority_items = sorted(set(ii_items) & set(user_items), key = ii_items.index)
    medium_priority_items += sorted(set(slim_items) & set(user_items), key = slim_items.index)
    medium_priority_items += sorted(set(ii_items) & set(slim_items), key = ii_items.index)

    # initial setting of final result -> priority to consensus
    final_list = remove_duplicates(max_priority_items + medium_priority_items)

    # remove already sampled items
    ii_items = remove_one_list_from_another(ii_items, final_list)
    user_items = remove_one_list_from_another(user_items, final_list)
    slim_items = remove_one_list_from_another(slim_items, final_list)

    while (len(final_list) < nRounds):

        coin = random.randint(0, 10)/10 #coin represents a probability
        print(coin)

        if coin <= 0.4: #pick from iiHybrid
            ii_picked_items = add_element_to_rec_list(ii_items, ii_picked_items, final_list)

        elif 0.4 < coin and coin < 0.7: #pick from user CF
            user_picked_items = add_element_to_rec_list(user_items, user_picked_items, final_list)

        else: #pick from slim
            slim_picked_items = add_element_to_rec_list(slim_items, slim_picked_items, final_list)


    final_list = remove_duplicates(final_list)

    return final_list[0:nRec]


def round_robin_merger(self, tupleDict, nRec, extra):
    """
        :param tupleDict : dict{(list_of_items, numAtEachRound)}
                            assumes list_of_items is ordered from best rec
                            to worst rec

        :param nRec      : number of items to recommend

        :return list of nRec items weighted according to dict
    """

    ii_items = tupleDict.get('ii')[0]
    n_ii = tupleDict.get('ii')[1]
    ii_picked_items = 0

    slim_items = tupleDict.get('slim')[0]
    n_slim = tupleDict.get('slim')[1]
    slim_picked_items = 0

    user_items = tupleDict.get('user')[0]
    n_user = tupleDict.get('user')[1]
    user_picked_items = 0

    final_list = []
    nRounds = nRec

    for round in range(nRounds):
        for i in range(n_ii):
            if (ii_picked_items < len(ii_items)):
                final_list.append(ii_items[round * n_ii + i])
                ii_picked_items += 1

        for i in range(n_slim):
            if (slim_picked_items < len(slim_items)):
                final_list.append(slim_items[round * n_slim + i])
                slim_picked_items += 1

        for i in range(n_user):
            if (user_picked_items < len(user_items)):
                final_list.append(user_items[round * n_user + i])
                user_picked_items += 1

    return self.remove_duplicates(final_list)[0:nRec]

#main

# lm = list_merger({'ii':([1,2,3,12,5,6,7,8,9,10]), 'user':([11,1,13,10,88,16,18,1,8,12]), 'slim':([36,34,37,33,31,39,38,35,8,72,12]) }, 10, 0)
#
# print(lm)