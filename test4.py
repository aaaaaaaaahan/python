state = {'page_cnt': 1, 'line_cnt': 0}
def print_header(branch):
    state['line_cnt'] = 9
    f.write(f"PAGE : {state['page_cnt']}")
