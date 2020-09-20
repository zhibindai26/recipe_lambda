from recipe_lambda import add_recipe, find_recipes


def main(event, context):
    action = event["action"].lower()

    if action == "add":
        response_obj = add_recipe(event)
    else:
        response_obj = find_recipes(event)

    return response_obj
