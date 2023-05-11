import math

from jsonlines import jsonlines
from tqdm import tqdm

def sigmoid(x):
  return 1 / (1 + math.exp(-x))

base=10000000000

with jsonlines.open('data/collnew2') as reader:
    for obj in tqdm(reader):
        name=obj['data']['collection_name']
        names=obj['template']['names']
        print(name)
        sorted1 = [x['normalized_name'] for x in sorted(names, key=lambda x: x['rank'], reverse=True)]
        sorted2 = [x['normalized_name'] for x in sorted(names, key=lambda x: x['system_interesting_score'], reverse=True)]
        # print([f"{x['rank']:.2f}" for x in names])
        sorted_new=sorted(names, key=lambda x: x['system_interesting_score']*math.log(x['rank']+1, base), reverse=True)
        sorted3 = [x['normalized_name'] for x in sorted_new]
        print('R\t', sorted1[:10])
        print('I\t', sorted2[:10])
        print('I*R\t', sorted3[:10])
        print([f"{x['rank']} {math.log(x['rank']+1, base)} {x['system_interesting_score']:.2f}={x['system_interesting_score']*math.log(x['rank']+1, base):.2f}" for x in sorted_new][:10])
        
        less=[]
        more=[]
        for x in sorted(names, key=lambda x: x['rank']):
            if x['rank']<100000:
                less.append(x)
            else:
                more.append(x)

        sorted4 = [x['normalized_name'] for x in sorted(more, key=lambda x: x['system_interesting_score'], reverse=True)] + [x['normalized_name'] for x in sorted(less, key=lambda x: x['rank'], reverse=True)]
        print('4\t', sorted4[:10])
        print()