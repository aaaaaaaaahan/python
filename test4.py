if first_page:
    # --- First page, no spacing before ---
    page_cnt += 1
    print_header(f, branch, page_cnt, report_date)
    current_branch = branch
    line_cnt = 9
    first_page = False

elif branch != current_branch or line_cnt >= 55:
    # --- Add spacing after each page before new one starts ---
    f.write("\n" * 3)
    page_cnt += 1
    print_header(f, branch, page_cnt, report_date)
    current_branch = branch
    line_cnt = 9
