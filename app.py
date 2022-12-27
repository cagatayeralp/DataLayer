from flask import Flask
from flask_restx import Resource, Api

api = Api()

app = Flask(__name__)
api.init_app(app)


@api.route('/test<id>')
@api.doc(params={'id': 'An ID'})
class MyResource(Resource):
    def get (self, id):
        return 'hello test' + id

    @api.doc(responses={403: 'Not Authorized'})
    def post(self, id):
        api.abort(403)

if __name__ == '__main__':
    app.run()
