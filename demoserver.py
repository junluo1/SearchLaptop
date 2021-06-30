import searchModel
from flask import Flask, request, render_template


app = Flask(__name__)


@app.route('/')
def my_form():
    return '''
    <!doctype html>
    <title>Query</title>
    <h1>Make a query</h1>
    <form method = "POST" >
        <input name = "text" style="width:500px">
        <input type = "submit" value = "Search">
    </form >
    '''


@app.route('/', methods=['POST'])
def my_form_post():
    text = request.form['text']
    # call the detectAttr to analyze the query
    searchQuery = searchModel.detectAttr(searchModel.attrs, text)
    # get the list of matched attribute titles
    fields = list(searchQuery.keys())
    # get the new query string that has filtered out the attribute titles
    text = " ".join(list(searchQuery.values()))
    # call the search method to retrieve docs based on new query and matched fields
    res = searchModel.obj.search(text, searchQuery)

    dct = {}
    for doc in res['hits']['hits']:
        dct[doc['_source']['Product']] = doc['_source']

    print(text)
    print("Located attributes: " + " ".join(fields))
    return dct


if __name__ == '__main__':
    app.run(debug=True)
