whead = 0
rhead = 0

----

send(m):

begin
    update where p = whead set message = m and whead >= rhead
    if(1)
        update whead = (whead + 1) % size
commit

----

receive():

begin
    update rhead = (rhead + 1) % size where rhead < whead
    if(1)
        select from message where pos = rhead

----

r = 0
w = 0

w = 1
r