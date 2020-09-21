from io import StringIO
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


def find_recipes(search_dict):
    """ Filter recipes based on provided search criteria """
    try:
        recipes_df = download_recipes()
        recipe = search_dict["Recipe"].lower()
        food_type = search_dict["Type"]
        ingredient = search_dict["Main_Ingredient"]
        cuisine = search_dict["Cuisine"]
        source = search_dict["Source"]
        sample = search_dict["Sample"]

        if recipe:
            recipe_df = recipes_df[recipes_df['Recipe'].str.contains(recipe, case=False, na=False)]
        else:
            recipe_df = recipes_df

        if food_type:
            type_df = recipe_df[recipe_df['Type'] == food_type]
        else:
            type_df = recipe_df

        if ingredient:
            ingredient_df = type_df[type_df['Ingredient'] == ingredient]
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

        if len(recipes_df) > 0:
            final_json = {
                "status_code": 200,
                "message": "Recipes found",
                "body": ""
            }
            if len(recipes_df) > sample:
                final_json["body"] = source_df.sample(sample).to_json(orient='records')
            else:
                final_json["body"] = source_df.to_json(orient='records')
            return final_json
        else:
            return {
                "status_code": 404,
                "message": "No recipes found",
                "body": ""
            }
    except Exception as e:
        return {
            "status_code": 400,
            "message": f"Recipe search failed: {e}"
        }


def add_recipe(new_recipe):
    """ Add a new recipe to the recipe list """
    try:
        recipes_df = download_recipes()
        updated_recipes_df = recipes_df.append(new_recipe, ignore_index=True)
        write_df_to_csv_on_s3(updated_recipes_df)
        return {
            "status_code": 200,
            "message": f"{new_recipe['Recipe']} added to recipes list"
        }
    except Exception as e:
        return {
            "status_code": 400,
            "message": f"Adding new recipe failed: {e}"
        }

