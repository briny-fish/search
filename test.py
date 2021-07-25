print(''.split(' '))
print(len(''))
import pandas as pd
import re
#outfile = open('submission.csv', 'w', encoding='utf-8')
tmpdict = {"qid":[],"doc":[]}
with open('submission_new_mbert.csv','r',encoding='utf-8') as f:
    cnt = 0
    for line in f.readlines():
        if cnt == 0:
            cnt += 1
            continue
        q = line[:line.index(',')]
        tmpdict['qid'].append(q)
        urls = line[line.index(',')+1:-1]
        if(urls!=''):
            urls = urls.split('')
        else:
            urls = []
        if(len(urls)!=10):print(len(urls))
        for x in range(len(urls)):
            urls[x] = re.sub(' http','http',urls[x])
        urls = ''.join(urls).split('')
        if(len(urls)<10):
            for i in range(10-len(urls)):
                urls.append('http')
        if len(urls)>10:
            urls = urls[:10]
        tmpdict['doc'].append(''.join(urls))
pd.DataFrame(tmpdict).to_csv('submission.csv')

        #outfile.write(q+','+''.join(urls)+'\n')
