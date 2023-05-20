import math

from jsonlines import jsonlines
from tqdm import tqdm
from scipy import stats


def sigmoid(x):
    return 1 / (1 + math.exp(-x))


base = 10000000000


def spear(a, b):
    return stats.spearmanr(a, b).correlation


def spear_sorting(a, b):
    return spear([x['normalized_name'] for x in a.order][:30], [x['normalized_name'] for x in b.order][:30])


class Sorting:
    def __init__(self, label, order):
        self.label = label
        self.order = order


def geometric_mean(a, b):
    return math.sqrt(a * b) if a * b > 0 else 0


def harmonic_mean(a, b):
    return 2 * a * b / (a + b) if a + b > 0 else 0


with jsonlines.open('data/collnew3') as reader:
    for obj in tqdm(reader, total=842):
        name = obj['data']['collection_name']
        names = obj['template']['names']
        print(name)

        sortings = []

        sorting_rank = Sorting('R', sorted(names, key=lambda x: x['rank'], reverse=True))
        sortings.append(sorting_rank)
        sortings.append(Sorting('I', sorted(names, key=lambda x: x['system_interesting_score'], reverse=True)))
        # sorted1 = [x['normalized_name'] for x in sorted(names, key=lambda x: x['rank'], reverse=True)]
        # sorted2 = [x['normalized_name'] for x in sorted(names, key=lambda x: x['system_interesting_score'], reverse=True)]
        # print([f"{x['rank']:.2f}" for x in names])

        sorted_new = sorted(names, key=lambda x: x['system_interesting_score'] * math.log(x['rank'] + 1, base),
                            reverse=True)
        # sorted3 = [x['normalized_name'] for x in sorted_new]
        sortings.append(Sorting('I*log(R)', sorted_new))

        # print('R\t', sorted1[:10])
        # print('I\t', sorted2[:10])
        # print('I*R\t', sorted3[:10])
        # print([f"{x['rank']} {math.log(x['rank']+1, base)} {x['system_interesting_score']:.2f}={x['system_interesting_score']*math.log(x['rank']+1, base):.2f}" for x in sorted_new][:10])

        sorting_len_rank = Sorting('len,rank', sorted(names, key=lambda x: (len(x['normalized_name']), -x['rank'])))
        # sorted_len_rank = [x['normalized_name'] for x in sorted_new]
        # print(f'len,rank', sorted_len_rank[:10])
        sortings.append(sorting_len_rank)

        less = []
        more = []
        for x in sorted(names, key=lambda x: x['rank']):
            if x['rank'] < 100000:
                less.append(x)
            else:
                more.append(x)

        # sorted4 = [x['normalized_name'] for x in sorted(more, key=lambda x: x['system_interesting_score'], reverse=True)] + [x['normalized_name'] for x in sorted(less, key=lambda x: x['rank'], reverse=True)]
        # print('4\t', sorted4[:10], spear(sorted4,sorted_rank), spear(sorted4,sorted_len_rank))
        sortings.append(Sorting('4',
                                list(sorted(more, key=lambda x: x['system_interesting_score'], reverse=True)) + list(
                                    sorted(less, key=lambda x: x['rank'], reverse=True))))

        # for power in range(0, 5):
        #     sorted_new = sorted(names, key=lambda x: x['rank'] / len(x['normalized_name']) ** power,
        #                         reverse=True)
        #     # sorted5 = [x['normalized_name'] for x in sorted_new]
        #     # print(f'rank/len**{power}\t', sorted5[:10], spear(sorted5,sorted1), spear(sorted5,sorted_len_rank))
        #     sortings.append(Sorting(f'rank/len**{power}', sorted_new))

        base = 2
        power = 1
        sorted_new = sorted(names, key=lambda x: math.log(x['rank'] + 1, base) / len(x['normalized_name']) ** power,
                            reverse=True)
        # sorted5 = [x['normalized_name'] for x in sorted_new]
        # print(f'log(rank,{base})/len**{power}\t', sorted5[:10], spear(sorted5,sorted1), spear(sorted5,sorted_len_rank))
        sortings.append(Sorting(f'log(rank,{base})/len**{power}', sorted_new))

        # for base in [2, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000]:
        #     for power in range(2, 5):
        #         sorted_new = sorted(names,
        #                             key=lambda x: math.log(x['rank'] + 1, base) / len(x['normalized_name']) ** power,
        #                             reverse=True)
        #         # sorted5 = [x['normalized_name'] for x in sorted_new]
        #         # print(f'log(rank,{base})/len**{power}\t', sorted5[:10], spear(sorted5,sorted1), spear(sorted5,sorted_len_rank))
        #         sortings.append(Sorting(f'log(rank,{base})/len**{power}', sorted_new))
        # 
        # for base in list(range(2, 15))+[0.7]:
        #     for power in list(range(1, 6))+[14]:
        #         sorted_new = sorted(names,
        #                             key=lambda x: (x['rank'] + 1) ** (1 / base) / len(x['normalized_name']) ** power,
        #                             reverse=True)
        #         # sorted5 = [x['normalized_name'] for x in sorted_new]
        #         # print(f'sqrt(rank,{base})/len**{power}\t', sorted5[:10], spear(sorted5,sorted1), spear(sorted5,sorted_len_rank))
        #         sortings.append(Sorting(f'sqrt(rank,{base})/len**{power}', sorted_new))

        for sorting in sortings:
            sorting.spearman_to_rank = spear_sorting(sorting, sorting_rank)
            sorting.spearman_to_len_rank = spear_sorting(sorting, sorting_len_rank)

        for sorting in sorted(sortings, key=lambda x: harmonic_mean(x.spearman_to_rank+1, x.spearman_to_len_rank+1), reverse=True):
            a = sorting.spearman_to_rank
            b = sorting.spearman_to_len_rank
            print(f'{a:.2f}',
                  f'{b:.2f}',
                  # f'{geometric_mean(a, b):.2f}',
                  f'{harmonic_mean(a+1, b+1)-1:.2f}',
                  sorting.label,
                  [x['normalized_name'] for x in sorting.order[:10]]
                  )

        print()
