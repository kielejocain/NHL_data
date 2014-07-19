import csv

import simplejson

year = raw_input("Season? (E.g., 2012-2013 = '13'): ")

filea = open(year + "goalsum.jl", 'r')
dicta = simplejson.load(filea)
filea.close()

filea = open(year + "goalps.jl", 'r')
dictb = simplejson.load(filea)
filea.close()

if int(year) > 5 and int(year) < 50:
    fileb = open(year + "goalso.jl", 'r')
    dictc = simplejson.load(fileb)
    fileb.close()

fileb = open(year + "goalst.jl", 'r')
dictd = simplejson.load(fileb)
fileb.close()

output = open(year + "goalies.csv", 'w+')

for k in dicta:
    for v in dictb:
        if k.setdefault('nhl_num', '1') == v.setdefault('nhl_num', '0'):
            k.update(v)
    if int(year) > 5 and int(year) < 50:
        for v in dictc:
            if k.setdefault('nhl_num', '1') == v.setdefault('nhl_num', '0'):
                k.update(v)
    for v in dictd:
        if k.setdefault('nhl_num', '1') == v.setdefault('nhl_num', '0'):
            k.update(v)

# simplejson.dump(dicta, output)

w = csv.DictWriter(output,dicta[0].keys())
w.writeheader()
for dct in dicta:
    w.writerow(dict((k, v.encode('utf-8') if isinstance(v, unicode) else v) for k,v in dct.iteritems()))

output.close()
