from io import StringIO
import json
import boto3
import pandas as pd

RECIPE_CSV = "recipes.csv"
S3_BUCKET = "zd-basic-bucket"


def download_recipes():
    """ Download recipe CSV from S3 and convert it to a Dataframe """

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=S3_BUCKET, Key=RECIPE_CSV)
    recipe_csv = response["Body"]

    df = pd.read_csv(recipe_csv, index_col=0).astype({'Page': str})
    return df


def write_df_to_csv_on_s3(df):
    """ Write a dataframe to a CSV on S3 """

    # Create buffer
    csv_buffer = StringIO()

    # Write dataframe to buffer
    df.to_csv(csv_buffer, sep=",")

    # Create S3 object
    s3 = boto3.resource("s3")

    # Write buffer to S3 object
    s3.Object(S3_BUCKET, RECIPE_CSV).put(Body=csv_buffer.getvalue())


def create_return_object(status_code, message, body):
    """ Create return object """

    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "message": message,
        "body": body
    }


def find_recipes(search_dict):
    """ Filter recipes based on provided search criteria """
    recipes_df = download_recipes()
    recipe = search_dict["recipe"].lower()
    food_type = search_dict["type"]
    ingredient = search_dict["main_ingredient"]
    cuisine = search_dict["cuisine"]
    source = search_dict["source"]
    sample = int(search_dict["sample"])
    get_categories = search_dict["get_categories"]
    body = {}

    if get_categories == 'true':
        # return lists of unique category values for dropdown lists
        meal_type_ls = sorted(recipes_df['Type'].dropna().unique())
        cuisine_ls = sorted(recipes_df['Cuisine'].dropna().unique())
        source_ls = sorted(recipes_df['Source'].dropna().unique())
        main_ingredient_ls = sorted(recipes_df['Main_Ingredient'].dropna().unique())
        
        empty_str = ""
        meal_type_ls.insert(0, empty_str)
        cuisine_ls.insert(0, empty_str)
        source_ls.insert(0, empty_str)
        main_ingredient_ls.insert(0, empty_str)
        
        status_code = 200
        message = "Returning Recipe Category Values"
        body['Type'] = meal_type_ls
        body['Cuisine'] = cuisine_ls
        body['Source'] = source_ls
        body['Main_Ingredient'] = main_ingredient_ls

    else:
        if not sample:
            sample = 5

        if recipe:
            recipe_df = recipes_df[recipes_df['Recipe'].str.contains(recipe, case=False, na=False)]
        else:
            recipe_df = recipes_df

        if food_type:
            type_df = recipe_df[recipe_df['Type'] == food_type]
        else:
            type_df = recipe_df

        if ingredient:
            ingredient_df = type_df[type_df['Main_Ingredient'] == ingredient]
        else:
            ingredient_df = type_df

        if cuisine:
            cuisine_df = ingredient_df[ingredient_df['Cuisine'] == cuisine]
        else:
            cuisine_df = ingredient_df

        if source:
            source_df = cuisine_df[cuisine_df['Source'] == source]
        else:
            source_df = cuisine_df

        if len(source_df) > 0:
            status_code = 200
            message = "Recipes found"
            if len(source_df) > sample:
                body['Recipes'] = json.loads(source_df.sample(sample).to_json(orient='records'))
            else:
                body['Recipes'] = json.loads(source_df.to_json(orient='records'))
        else:
            status_code = 400
            message = "Recipes not found"
            body['Recipes'] = ""

    return create_return_object(status_code, message, body)


def add_recipe(new_recipe):
    """ Add a new recipe to the recipe list """

    recipes_df = download_recipes()
    new_recipe_cleaned = clean_new_recipe_dict(new_recipe)

    if new_recipe_cleaned["Recipe"]:
        updated_recipes_df = recipes_df.append(new_recipe_cleaned, ignore_index=True)
        write_df_to_csv_on_s3(updated_recipes_df)
        message = f"{new_recipe['Recipe']} added to recipes list"
        return create_return_object(200, message, "")
    else:
        message = "Recipe Name is required"
        return create_return_object(400, message, "")


def clean_new_recipe_dict(new_recipe_dict):
    """ Extract only the needed values from the new recipe object """

    return {
        "Recipe": new_recipe_dict["Recipe"],
        "Type": new_recipe_dict["Type"],
        "Main_Ingredient": new_recipe_dict["Main_Ingredient"],
        "Cuisine": new_recipe_dict["Cuisine"],
        "Source": new_recipe_dict["Source"],
        "Page": new_recipe_dict["Page"],
        "Link": new_recipe_dict["Link"]
        }
