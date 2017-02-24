#pylint: disable=too-many-arguments

def inject_value(
    input_data,
    output_data,
    from_path,
    to_path,
    filter_function,
    from_index=0,
    to_index=0
):
    """
    injects parts of output_data into input_data
    based on from_path and to_path
    """

    if from_index < len(from_path):
        next_input_field = from_path[from_index]
        next_output_field = to_path[to_index]
        from_index += 1
        to_index += 1

        input_data = input_data.get(next_input_field)

        if from_index == len(from_path):
            output_data[next_output_field] = input_data
        elif isinstance(input_data, dict):
            output_row = dict()
            output_data[next_output_field] = output_row
            inject_value(
                input_data,
                output_row,
                from_path,
                to_path,
                filter_function,
                from_index=from_index,
                to_index=to_index
            )

        elif isinstance(input_data, list):
            if len(input_data) > 0 and isinstance(input_data[0], dict):
                output_data[next_output_field] = []
                for input_row in input_data:
                    output_row = {}
                    output_data[next_output_field].append(output_row)
                    inject_value(
                        input_row,
                        output_row,
                        from_path,
                        to_path,
                        filter_function,
                        from_index=from_index,
                        to_index=to_index
                    )
            else:
                output_data[next_output_field] = [
                    filter_function(value) for value in input_data
                ]


        else:
            output_data[next_output_field] = filter_function(input_data)
