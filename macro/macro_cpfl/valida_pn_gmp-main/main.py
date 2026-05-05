import tkinter as tk
from interface.interface_principal import InterfacePrincipal
from config import config_usuario

if __name__ == "__main__":
    root = tk.Tk()
    app = InterfacePrincipal(root, config_usuario)
    root.mainloop()