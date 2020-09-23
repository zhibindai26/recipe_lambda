from recipe_lambda import add_recipe, find_recipes


def main(event, context):
    method = event['requestContext']['http']['method']

    if method == "POST":
        response_obj = add_recipe(event)
    else:
        response_obj = find_recipes(event)
    return response_obj
