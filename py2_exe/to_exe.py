from PyInstaller.__main__ import run




def py2exe_to_only_exe():
    opts = [r'D:\AD-Helper1\ad_helper\recommend\py2_exe\py2exe.py',
            '-F', '-w', r'--distpath=D:\AD-Helper1\ad_helper\recommend\py2_exe\monthly_upload',
            r'--workpath=D:\AD-Helper1\ad_helper\recommend\py2_exe\monthly_upload\history',
            r'--specpath=D:\AD-Helper1\ad_helper\recommend\py2_exe\monthly_upload\build',
            r'-n=Month_Data_Sum_PRO',]
    run(opts)


if __name__ == "__main__":
    py2exe_to_only_exe()
