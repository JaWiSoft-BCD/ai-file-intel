def progress_tracker(total:int, done:int):
    persentage_done = round(done/total*100, 2)
    print(f"{str(done)} out of {str(total)} done. Completed: {persentage_done}%")