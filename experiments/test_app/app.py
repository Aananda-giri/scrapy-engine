from flask import Flask, render_template

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/page1")
def page1():
    return "<h1>Page 1 Content</h1><a href='/'>Back to Home</a>"

@app.route("/page2")
def page2():
    return "<h1>Page 2 Content</h1><a href='/'>Back to Home</a>"

@app.route("/page3")
def page3():
    return "<h1>Page 3 Content</h1><a href='/'>Back to Home</a>"

@app.route("/nested/page4")
def page4():
    return "<h1>Nested Page 4 Content</h1><a href='/'>Back to Home</a>"

@app.route("/page5")
def page5():
    return "<h1>Page 5 Content</h1><a href='/'>Back to Home</a>"

@app.route("/footer_link")
def footer_link():
    return "<h1>Footer Link Content</h1><a href='/'>Back to Home</a>"

@app.route("/relative/path/page7")
def page7():
    return "<h1>Relative Path Page 7 Content</h1><a href='/'>Back to Home</a>"

if __name__ == "__main__":
    app.run(debug=True)