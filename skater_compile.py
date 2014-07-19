import csv

import simplejson

year = raw_input("Season? (E.g., 2012-2013 = '13'): ")

filea = open(year + "skatsum.jl", 'r')
dicta = simplejson.load(filea)
filea.close()

filea = open(year + "skateng.jl", 'r')
dictb = simplejson.load(filea)
filea.close()

fileb = open(year + "skatpim.jl", 'r')
dictc = simplejson.load(fileb)
fileb.close()

fileb = open(year + "skatpm.jl", 'r')
dictd = simplejson.load(fileb)
fileb.close()

fileb = open(year + "skatrts.jl", 'r')
dicte = simplejson.load(fileb)
fileb.close()

if int(year) > 5 and int(year) < 50:
    fileb = open(year + "skatso.jl", 'r')
    dictf = simplejson.load(fileb)
    fileb.close()

fileb = open(year + "skatot.jl", 'r')
dictg = simplejson.load(fileb)
fileb.close()

fileb = open(year + "skattoi.jl", 'r')
dicth = simplejson.load(fileb)
fileb.close()



output = open(year + "skaters.csv", 'w+')

for k in dicta:
    for v in dictb:
        if k.setdefault('nhl_num', '1') == v.setdefault('nhl_num', '0'):
            k.update(v)
    for v in dictc:
        if k.setdefault('nhl_num', '1') == v.setdefault('nhl_num', '0'):
            k.update(v)
    for v in dictd:
        if k.setdefault('nhl_num', '1') == v.setdefault('nhl_num', '0'):
            k.update(v)
    for v in dicte:
        if k.setdefault('nhl_num', '1') == v.setdefault('nhl_num', '0'):
            k.update(v)
    if int(year) > 5 and int(year) < 50:
        for v in dictf:
            if k.setdefault('nhl_num', '1') == v.setdefault('nhl_num', '0'):
                k.update(v)
    for v in dictg:
        if k.setdefault('nhl_num', '1') == v.setdefault('nhl_num', '0'):
            k.update(v)
    for v in dicth:
        if k.setdefault('nhl_num', '1') == v.setdefault('nhl_num', '0'):
            k.update(v)

# simplejson.dump(dicta, output)

w = csv.DictWriter(output,dicta[0].keys())
w.writeheader()
for dct in dicta:
    w.writerow(dict((k, v.encode('utf-8') if isinstance(v, unicode) else v) for k,v in dct.iteritems()))

output.close()
