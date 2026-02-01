# Dataset schema specification

**Learn how to define and present your dataset schema in an user-friendly output UI.**

***

The dataset schema defines the structure and representation of data produced by an Actor, both in the API and the visual user interface.

## Example

Let's consider an example Actor that calls `Actor.pushData()` to store data into dataset:

main.js


```
import { Actor } from 'apify';
// Initialize the JavaScript SDK
await Actor.init();

/**
 * Actor code
 */
await Actor.pushData({
    numericField: 10,
    pictureUrl: 'https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_92x30dp.png',
    linkUrl: 'https://google.com',
    textField: 'Google',
    booleanField: true,
    dateField: new Date(),
    arrayField: ['#hello', '#world'],
    objectField: {},
});


// Exit successfully
await Actor.exit();
```


To set up the Actor's output tab UI using a single configuration file, use the following template for the `.actor/actor.json` configuration:

.actor/actor.json


```
{
    "actorSpecification": 1,
    "name": "Actor Name",
    "title": "Actor Title",
    "version": "1.0.0",
    "storages": {
        "dataset": {
            "actorSpecification": 1,
            "views": {
                "overview": {
                    "title": "Overview",
                    "transformation": {
                        "fields": [
                            "pictureUrl",
                            "linkUrl",
                            "textField",
                            "booleanField",
                            "arrayField",
                            "objectField",
                            "dateField",
                            "numericField"
                        ]
                    },
                    "display": {
                        "component": "table",
                        "properties": {
                            "pictureUrl": {
                                "label": "Image",
                                "format": "image"
                            },
                            "linkUrl": {
                                "label": "Link",
                                "format": "link"
                            },
                            "textField": {
                                "label": "Text",
                                "format": "text"
                            },
                            "booleanField": {
                                "label": "Boolean",
                                "format": "boolean"
                            },
                            "arrayField": {
                                "label": "Array",
                                "format": "array"
                            },
                            "objectField": {
                                "label": "Object",
                                "format": "object"
                            },
                            "dateField": {
                                "label": "Date",
                                "format": "date"
                            },
                            "numericField": {
                                "label": "Number",
                                "format": "number"
                            }
                        }
                    }
                }
            }
        }
    }
}
```


The template above defines the configuration for the default dataset output view. Under the `views` property, there is one view titled *Overview*. The view configuration consists of two main steps:

1. `transformation` - set up how to fetch the data.
2. `display` - set up how to visually present the fetched data.

The default behavior of the Output tab UI table is to display all fields from `transformation.fields` in the specified order. You can customize the display properties for specific formats or column labels if needed.

![Output tab UI](/assets/images/output-schema-example-42bf91c1c1f39834fad5bbedf209acaa.png)

## Structure

Output configuration files need to be located in the `.actor` folder within the Actor's root directory.

You have two choices of how to organize files within the `.actor` folder.

### Single configuration file

.actor/actor.json


```
{
    "actorSpecification": 1,
    "name": "this-is-book-library-scraper",
    "title": "Book Library scraper",
    "version": "1.0.0",
    "storages": {
        "dataset": {
            "actorSpecification": 1,
            "fields": {},
            "views": {
                "overview": {
                    "title": "Overview",
                    "transformation": {},
                    "display": {}
                }
            }
        }
    }
}
```


### Separate configuration files

.actor/actor.json


```
{
    "actorSpecification": 1,
    "name": "this-is-book-library-scraper",
    "title": "Book Library scraper",
    "version": "1.0.0",
    "storages": {
        "dataset": "./dataset_schema.json"
    }
}
```


.actor/dataset\_schema.json


```
{
    "actorSpecification": 1,
    "fields": {},
    "views": {
        "overview": {
            "title": "Overview",
            "transformation": {},
            "display": {
                "component": "table"
            }
        }
    }
}
```


Both of these methods are valid so choose one that suits your needs best.

## Handle nested structures

The most frequently used data formats present the data in a tabular format (Output tab table, Excel, CSV). If your Actor produces nested JSON structures, you need to transform the nested data into a flat tabular format. You can flatten the data in the following ways:

* Use `transformation.flatten` to flatten the nested structure of specified fields. This transforms the nested object into a flat structure. e.g. with `flatten:["foo"]`, the object `{"foo": {"bar": "hello"}}` is turned into `{"foo.bar": "hello"}`. Once the structure is flattened, it's necessary to use the flattened property name in both `transformation.fields` and `display.properties`, otherwise, fields might not be fetched or configured properly in the UI visualization.

* Use `transformation.unwind` to deconstruct the nested children into parent objects.

* Change the output structure in an Actor from nested to flat before the results are saved in the dataset.

## Dataset schema structure definitions

The dataset schema structure defines the various components and properties that govern the organization and representation of the output data produced by an Actor. It specifies the structure of the data, the transformations to be applied, and the visual display configurations for the Output tab UI.

### DatasetSchema object definition

| Property             | Type                         | Required | Description                                                                                                  |
| -------------------- | ---------------------------- | -------- | ------------------------------------------------------------------------------------------------------------ |
| `actorSpecification` | integer                      | true     | Specifies the version of dataset schema<br />structure document.<br />Currently only version 1 is available. |
| `fields`             | JSONSchema compatible object | true     | Schema of one dataset object.<br />Use JsonSchema Draft 2020–12 or<br />other compatible formats.            |
| `views`              | DatasetView object           | true     | An object with a description of an API<br />and UI views.                                                    |

### DatasetView object definition

| Property         | Type                      | Required | Description                                                                                           |
| ---------------- | ------------------------- | -------- | ----------------------------------------------------------------------------------------------------- |
| `title`          | string                    | true     | The title is visible in UI in the Output tab<br />and in the API.                                     |
| `description`    | string                    | false    | The description is only available in the API response.                                                |
| `transformation` | ViewTransformation object | true     | The definition of data transformation<br />applied when dataset data is loaded from<br />Dataset API. |
| `display`        | ViewDisplay object        | true     | The definition of Output tab UI visualization.                                                        |

### ViewTransformation object definition

| Property  | Type      | Required | Description                                                                                                                                                                                                      |
| --------- | --------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fields`  | string\[] | true     | Selects fields to be presented in the output.<br />The order of fields matches the order of columns<br />in visualization UI. If a field value<br />is missing, it will be presented as **undefined** in the UI. |
| `unwind`  | string\[] | false    | Deconstructs nested children into parent object,<br />For example, with `unwind:["foo"]`, the object `{"foo": {"bar": "hello"}}`<br />is transformed into `{"bar": "hello"}`.                                    |
| `flatten` | string\[] | false    | Transforms nested object into flat structure.<br />For example, with `flatten:["foo"]` the object `{"foo":{"bar": "hello"}}`<br />is transformed into `{"foo.bar": "hello"}`.                                    |
| `omit`    | string\[] | false    | Removes the specified fields from the output.<br />Nested fields names can be used as well.                                                                                                                      |
| `limit`   | integer   | false    | The maximum number of results returned.<br />Default is all results.                                                                                                                                             |
| `desc`    | boolean   | false    | By default, results are sorted in ascending based on the write event into the dataset.<br />If `desc:true`, the newest writes to the dataset will be returned first.                                             |

### ViewDisplay object definition

| Property     | Type   | Required | Description                                                                                                                                                                                                                       |
| ------------ | ------ | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `component`  | string | true     | Only the `table` component is available.                                                                                                                                                                                          |
| `properties` | Object | false    | An object with keys matching the `transformation.fields`<br />and `ViewDisplayProperty` as values. If properties are not set, the table will be rendered automatically with fields formatted as `strings`, `arrays` or `objects`. |

### ViewDisplayProperty object definition

| Property | Type                                                                                                                       | Required | Description                                                                         |
| -------- | -------------------------------------------------------------------------------------------------------------------------- | -------- | ----------------------------------------------------------------------------------- |
| `label`  | string                                                                                                                     | false    | In the Table view, the label will be visible as the table column's header.          |
| `format` | One of - `text`<br />- `number`<br />- `date`<br />- `link`<br />- `boolean`<br />- `image`<br />- `array`<br />- `object` | false    | Describes how output data values are formatted to be rendered in the Output tab UI. |


# Dataset validation

**Specify the dataset schema within the Actors so you can add monitoring and validation at the field level.**

***

To define a schema for a default dataset of an Actor run, you need to set `fields` property in the dataset schema.

info

The schema defines a single item in the dataset. Be careful not to define the schema as an array, it always needs to be a schema of an object.

Schema configuration is not available for named datasets or dataset views.

You can either do that directly through `actor.json`:

.actor.json


```
{
    "actorSpecification": 1,
    "storages": {
        "dataset": {
            "actorSpecification": 1,
            "fields": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string"
                    }
                },
                "required": ["name"]
            },
            "views": {}
        }
    }
}
```


Or in a separate file linked from the `.actor.json`:

.actor.json


```
{
    "actorSpecification": 1,
    "storages": {
        "dataset": "./dataset_schema.json"
    }
}
```


dataset\_schema.json


```
{
    "actorSpecification": 1,
    "fields": {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "name": {
                "type": "string"
            }
        },
        "required": ["name"]
    },
    "views": {}
}
```


important

Dataset schema needs to be a valid JSON schema draft-07, so the `$schema` line is important and must be exactly this value or it must be omitted:

`"$schema": "http://json-schema.org/draft-07/schema#"`

## Dataset validation

When you define a schema of your default dataset, the schema is then always used when you insert data into the dataset to perform validation (we use [AJV](https://ajv.js.org/)).

If the validation succeeds, nothing changes from the current behavior, data is stored and an empty response with status code `201` is returned.

If the data you attempt to store in the dataset is *invalid* (meaning any of the items received by the API fails validation), *the entire request will be discarded*, The API will return a response with status code `400` and the following JSON response:


```
{
    "error": {
        "type": "schema-validation-error",
        "message": "Schema validation failed",
        "data": {
            "invalidItems": [{
                "itemPosition": "<array index in the received array of items>",
                "validationErrors": "<Complete list of AJV validation error objects>"
            }]
        }
    }
}
```


For the complete AJV validation error object type definition, refer to the [AJV type definitions on GitHub](https://github.com/ajv-validator/ajv/blob/master/lib/types/index.ts#L86).

If you use the Apify JS client or Apify SDK and call `pushData` function you can access the validation errors in a `try catch` block like this:

* Javascript
* Python


```
try {
    const response = await Actor.pushData(items);
} catch (error) {
    if (!error.data?.invalidItems) throw error;
    error.data.invalidItems.forEach((item) => {
        const { itemPosition, validationErrors } = item;
    });
}
```



```
from apify import Actor
from apify_client.errors import ApifyApiError

async with Actor:
    try:
        await Actor.push_data(items)
    except ApifyApiError as error:
        if 'invalidItems' in error.data:
            validation_errors = error.data['invalidItems']
```


## Examples of common types of validation

Optional field (price is optional in this case):


```
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "price": {
            "type": "number"
        }
    },
    "required": ["name"]
}
```


Field with multiple types:


```
{
    "price": {
        "type": ["string", "number"]
    }
}
```


Field with type `any`:


```
{
    "price": {
        "type": ["string", "number", "object", "array", "boolean"]
    }
}
```


Enabling fields to be `null` :


```
{
    "name": {
        "type": ["string", "null"]
    }
}
```


In case of enums `null` needs to be within the set of allowed values:


```
{
    "type": {
        "enum": ["list", "detail", null]
    }
}
```


Define type of objects in array:


```
{
    "comments": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "author_name": {
                    "type": "string"
                }
            }
        }
    }
}
```


Define specific fields, but allow anything else to be added to the item:


```
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        }
    },
    "additionalProperties": true
}
```


See [json schema reference](https://json-schema.org/understanding-json-schema/reference) for additional options.

You can also use [conversion tools](https://www.liquid-technologies.com/online-json-to-schema-converter) to convert an existing JSON document into it's JSON schema.

## Dataset field statistics

When you configure the dataset fields schema, we generate a field list and measure the following statistics:

* **Null count:** how many items in the dataset have the field set to null

* **Empty count:** how many items in the dataset are `undefined` , meaning that for example empty string is not considered empty

* **Minimum and maximum**

  * For numbers, this is calculated directly
  * For strings, this field tracks string length
  * For arrays, this field tracks the number of items in the array
  * For objects, this tracks the number of keys
  * For booleans, this tracks whether the boolean was set to true. Minimum is always 0, but maximum can be either 1 or 0 based on whether at least one item in the dataset has the boolean field set to true.

You can use them in [monitoring](https://docs.apify.com/platform/monitoring.md#alert-configuration).
