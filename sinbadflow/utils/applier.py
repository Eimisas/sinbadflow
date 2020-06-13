def apply_conditional_func(pipeline, f):
    '''Applies conditional function to every agent of the pipeline.

    Args:
      pipeline: BaseAgent object
      f: function with Boolean return type

    Returns:
      pipeline (BaseAgent object) with f applied to conditional_func parameter
    '''
    pointer = pipeline
    while pointer is not None:
        for elem in pointer.data:
            elem.conditional_func = f
        pointer = pointer.prev_elem
    return pipeline
