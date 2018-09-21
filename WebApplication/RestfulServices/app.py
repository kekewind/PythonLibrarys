from flask import Flask
from flask import request
from flask import render_template

app = Flask(__name__, static_url_path='')
api_url = "/Restful-Services/services/api"
base_url = ""


@app.route('/Restful-Services', methods=['GET', 'POST'])
def home():
    global base_url
    base_url = request.base_url[0:len(request.base_url)] + "/services/api"
    return render_template('index.html', url=base_url, api=base_url)


@app.route(api_url, methods=['GET', 'POST'])
def api_index():
    resources = []
    for rule in app.url_map.iter_rules():
        options = {}
        paras = []
        for arg in rule.arguments:
            options[arg] = "[{0}]".format(arg)
            paras.append(arg)
        resources.append(
            {
                "methods": ','.join(rule.methods),
                "url": rule.rule,
                "paras": paras,
                "func": rule.endpoint,
                "response_type": ["application/xml", 'application/json']
            }
        )
    return render_template("API.html", resources=resources), {'Content-Type': 'application/xml'}


@app.route(api_url + '/upload.do', methods=['POST'])
def file_upload():
    if request.method == 'POST':
        if request.form["name"]:
            blob = request.files['file'].stream.read()
            try:
                with open(request.form["name"], "ab+") as f:
                    f.write(bytes(blob))
                    f.flush()
                    f.close()
            except IOError:
                assert IOError.strerror
            return "uploading:" + str((int(request.form["index"]) + 1) * 10 * 1024 * 1024), response_build(f)
        else:
            return 'Invalid upload'


def response_build(f):
    if f == 'json':
        return {'Content-Type': 'application/json'}
    elif f == "xml":
        return {'Content-Type': 'application/xml'}
    else:
        return {'Content-Type': 'application/html'}


if __name__ == '__main__':
    app.run(port=8082)
