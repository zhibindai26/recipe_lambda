from recipe_lambda import add_recipe, find_recipes
import json


def main(event, context):

    try:
        method = event['http_method']

        if method == "POST":
            response_obj = add_recipe(event)
        else:
            response_obj = find_recipes(event)
        return response_obj
    except Exception as e:
        error_msg = {
            "error": "Exception",
            "message": "Lambda failed with the following error: " + str(e)
        }
        raise Exception(json.dumps(error_msg))
