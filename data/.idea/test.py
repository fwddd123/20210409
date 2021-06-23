import turtle
t=turtle.Pen()
for i in range(5):
        t.penup()
        t.goto(0,-i*50)
        t.pendown()
        t.circle((i+1)*50)
turtle.done()