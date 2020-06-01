from ..element import Element

def apply_condition_func(pipeline, f):
    '''Applies conditional function to the whole pipeline.
    Inputs:
        pipeline: BaseAgent
        f: function

    Returns:
        pipeline (BaseAgent) with f applied to conditional_func parameter    
    '''
    pointer = pipeline
    while pointer is not None:
        for elem in pointer.data:
            elem.is_conditional_func_passed = f
        pointer = pointer.prev_elem
    return pipeline
    
