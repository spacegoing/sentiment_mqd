# -*- coding: utf-8 -*-
import pandas as pd
gen_tmp = 'curl http://localhost:6800/schedule.json -d project=guba_spiders -d spider=guba -d fname=part%d.pickle'

create_cmds = []
for i in range(89):
  create_cmds.append(gen_tmp%i)

s = pd.Series(create_cmds)
s.to_csv('create.sh', index=None)




