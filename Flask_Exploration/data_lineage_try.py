from flask import Flask, render_template

app = Flask(__name__)

# Example Data Lineage
data_lineage = {
    "final_table": {
        "columns": {
            "final_column": {
                "derived_from": {
                    "intermediate_column": {
                        "derived_from": {
                            "source_column": {
                                "source_table": "raw.source_table",
                                "transformation_details": "Transformation Details: raw.source_table.transformation_details"
                            }
                        }
                    }
                }
            },
            "intermediate_column": {
                "derived_from": {
                    "source_column": {
                        "source_table": "raw.source_table",
                        "transformation_details": "Transformation Details: raw.source_table.transformation_details"
                    }
                }
            },
            "source_column": {
                "source_table": "raw.source_table",
                "transformation_details": "Transformation Details: raw.source_table.transformation_details"
            },
            "transformation_details": {
                "source_table": "raw.source_table",
                "transformation_details": "Transformation Details: raw.source_table.transformation_details"
            }
        }
    }
}

@app.route('/')
def index():
    return render_template('index.html', data_lineage=data_lineage)

if __name__ == '__main__':
    app.run(debug=True)
