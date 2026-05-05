import sys

from cx_Freeze import setup, Executable

# setup.py
# gerar exe ==> python setup.py build

build_exe_options = {
    "packages": ["os", "tkinter", "selenium", "datetime", "time", "csv", "queue", "pathlib", "dotenv",
                 "webdriver_manager", "pygame"],
    "includes": [
        "selenium.webdriver.common.by",
        "selenium.webdriver.support.ui",
        "selenium.webdriver.support.expected_conditions",
        "selenium.webdriver.common.action_chains",
        "selenium.common.exceptions"
    ],
    "include_files": [
        "assets/alerta.mp3", "assets/alerta.mp3", 
        "assets/valida.ico", "assets/valida.ico" ,
        ".env"
    ],  # se precisar já incluir
    "include_msvcr": True,  # inclui dlls do Visual C++ runtime
    
}

base = None # None para windows, "Win32GUI" para windows sem console
if sys.platform == "win32":
    base = "gui" # "Win32GUI" não usa mais

setup(
    name="ValidaPN_GMP",
    version="3.0",
    description="Validação automática de PN CPFL",
    options={"build_exe": build_exe_options},
    executables=[Executable("main.py", base=base, target_name="ValidaPN_GMP", icon="valida.ico")],
)
