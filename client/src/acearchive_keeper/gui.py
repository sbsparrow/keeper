from concurrent.futures import ProcessPoolExecutor
from multiprocessing import freeze_support, Manager
from threading import Thread
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
from queue import Empty

import logging
import logging.handlers

from acearchive_keeper.configure import BackupOptionsModel, ConfigFileModel, KeeperModel, get_config_path, read_config, write_config
from acearchive_keeper.worker import run_gui_worker, ACEARCHIVE_API_URI, ACEARCHIVE_BACKUPS_API_URI, ACEARCHIVE_CHECKSUM_API_URI
from acearchive_keeper.utils import load_frozen_certs, setup_gui_logger

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class TkLogHandler(logging.Handler):
    """
    Logging handler that writes log messages into a Tkinter Text widget.
    """

    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        self.text_widget.after(0, self._append, msg)

    def _append(self, msg):
        self.text_widget.config(state="normal")
        self.text_widget.insert("end", msg + "\n")
        self.text_widget.see("end")
        self.text_widget.config(state="disabled")


def pick_file():
    filename = filedialog.askopenfilename(
        title="Choose a zip to use for incremental backup",
        initialfile="keeper.zip",
        defaultextension="zip",
    )
    if filename:
        file_var.set(filename)


def set_entry_field_state(entry: tk.Entry, state) -> None:
    if state == "disabled":
        entry.configure(state="disabled", style="Disabled.TEntry")
    else:
        entry.configure(state="normal", style="TEntry")

def set_button_state(button: tk.Button, state) -> None:
    if state == "disabled":
        button.configure(state="disabled", style="Disabled.TButton")
    else:
        button.configure(state="normal", style="TButton")

def set_checkbutton_state(button: tk.Checkbutton, state) -> None:
    if state == "disabled":
        button.configure(state="disabled", style="Disabled.TCheckbutton")
    else:
        button.configure(state="normal", style="TCheckbutton")



def disable_ui():
    for entry_field in [file_entry, email_entry, logfile_entry]:
        set_entry_field_state(entry_field, "disabled")

    set_button_state(pick_button, "disabled")
    set_checkbutton_state(verbose_check, "disabled")


def enable_ui():
    for entry_field in [file_entry, email_entry, logfile_entry]:
        set_entry_field_state(entry_field, "enabled")
    set_button_state(pick_button, "enabled")
    set_checkbutton_state(verbose_check, "enabled")


def go():

    global progress_queue
    global log_queue
    global current_future

    disable_ui()
    clear_output()
    progress_bar['value'] = 0
    go_button.config(text="STOP", command=stop)

    keeper_id = id_var.get()
    keeper_email = email_var.get()
    zip_path = file_var.get()
    log_file = logfile_var.get()
    log_verbose = verbose_var.get()
    src_url = src_url = ACEARCHIVE_API_URI
    checksum_api_url = ACEARCHIVE_CHECKSUM_API_URI
    backup_api_url = ACEARCHIVE_BACKUPS_API_URI

    config = ConfigFileModel(
        BackupOptions=BackupOptionsModel(zip_file=zip_path,
                                         log_file=log_file,
                                         log_verbose=log_verbose),
        Keeper=KeeperModel(id=keeper_id,
                           email=keeper_email)
    )
    write_options_to_config(config)

    future = executor.submit(
        run_gui_worker,
        keeper_id,
        keeper_email,
        zip_path,
        src_url,
        checksum_api_url,
        backup_api_url,
        cancel_event,
        log_queue,
        progress_queue,
        log_verbose
    )
    future.add_done_callback(on_worker_done)
    current_future = future


def stop():
    if messagebox.askyesno("Cancel Backup",
                           "The archive backup is still running. Cancel anyway?",
                           icon="warning"):
        cancel_event.set()
        current_future.cancel()
        logger.info("Canceled Future (as seen from GUI)")
    else:
        return


def main():
    global executor, manager, log_queue, progress_queue, cancel_event
    global file_var, id_var, email_var, verbose_var, logfile_var
    global file_entry, id_entry, email_entry, verbose_check, logfile_entry
    global pick_button, go_button, output_text, progress_bar
    global queue_listener, root
    global current_future

    current_future = None

    config = read_options_from_config()

    root = tk.Tk()
    root.title("Ace Archive Keeper")
    root.geometry("1280x960")

    root.columnconfigure(0, weight=0)
    root.columnconfigure(1, weight=1)
    root.rowconfigure(0, weight=1)

    disabled_style = ttk.Style()
    disabled_style.configure(
        "Disabled.TEntry",
        foreground="#808080",
        fieldbackground="#f0f0f0",
        background="#f0f0f0"
    )
    disabled_style = ttk.Style()
    disabled_style.configure(
        "Disabled.TButton",
        foreground="#808080",
        background="#f0f0f0"
    )
    disabled_style.configure(
        "Disabled.TCheckbutton",
        foreground="#808080",
        background="#f0f0f0"
    )

    left_frame = ttk.Frame(root, padding=10, width=375)
    left_frame.grid(row=0, column=0, sticky="nsw")
    left_frame.grid_propagate(False)

    right_frame = ttk.Frame(root, padding=10)
    right_frame.grid(row=0, column=1, sticky="nsew")

    file_var = tk.StringVar()
    id_var = tk.StringVar()
    email_var = tk.StringVar()
    verbose_var = tk.BooleanVar()
    logfile_var = tk.StringVar()

    ttk.Label(left_frame, text="File").grid(row=0, column=0, sticky="w")
    file_entry = ttk.Entry(left_frame, textvariable=file_var, width=40)
    file_entry.insert(0, config.BackupOptions.zip_file)
    file_entry.grid(row=1, column=0, sticky="ew")

    pick_button = ttk.Button(left_frame, text="Pick File", command=pick_file)
    pick_button.grid(row=2, column=0, pady=(5, 15), sticky="w")

    ttk.Label(left_frame, text="ID").grid(row=3, column=0, sticky="w")
    id_entry = ttk.Entry(left_frame, textvariable=id_var, style="Disabled.TEntry")
    id_entry.insert(0, config.Keeper.id)
    id_entry.config(state="readonly")
    id_entry.grid(row=4, column=0, sticky="ew", pady=(0, 10))

    ttk.Label(left_frame, text="Email").grid(row=5, column=0, sticky="w")
    email_entry = ttk.Entry(left_frame, textvariable=email_var)
    email_entry.insert(0, config.Keeper.email)
    email_entry.grid(row=6, column=0, sticky="ew", pady=(0, 10))

    verbose_check = ttk.Checkbutton(left_frame, text="Log Verbose?", variable=verbose_var)
    verbose_var.set(value=config.BackupOptions.log_verbose)
    verbose_check.grid(row=7, column=0, sticky="w", pady=(0, 10))

    ttk.Label(left_frame, text="Log File").grid(row=8, column=0, sticky="w")
    logfile_entry = ttk.Entry(left_frame, textvariable=logfile_var)
    logfile_entry.insert(0, config.BackupOptions.log_file)
    logfile_entry.grid(row=9, column=0, sticky="ew")

    left_frame.columnconfigure(0, weight=1)

    ttk.Label(right_frame, text="Output").grid(row=0, column=0, sticky="w")

    output_text = scrolledtext.ScrolledText(right_frame, height=20, state="disabled", wrap="word")
    output_text.grid(row=1, column=0, sticky="nsew")

    right_frame.rowconfigure(1, weight=1)
    right_frame.columnconfigure(0, weight=1)

    bottom_frame = ttk.Frame(root, padding=10)
    bottom_frame.grid(row=1, column=0, columnspan=2, sticky="ew")

    bottom_frame.columnconfigure(0, weight=1)

    progress_bar = ttk.Progressbar(bottom_frame, mode="determinate", maximum=100)
    progress_bar.grid(row=0, column=0, sticky="ew", pady=(0, 10))

    go_button = ttk.Button(bottom_frame, text="GO", command=go)
    go_button.grid(row=1, column=0)



    manager = Manager()
    log_queue = manager.Queue()
    progress_queue = manager.Queue()
    cancel_event = manager.Event()
    executor = ProcessPoolExecutor(max_workers=1)
    setup_gui_logger(logger, log_queue)

    # Logging setup

    gui_handler = TkLogHandler(output_text)
    #gui_handler.setFormatter(logging.Formatter(fmt="%(asctime)s %(message)s", datefmt="%x %X"))

    queue_listener = logging.handlers.QueueListener(
        log_queue,
        gui_handler
    )
    queue_listener.start()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.after(100, poll_progress)
    root.mainloop()


def on_worker_done(fut):
    enable_ui()
    go_button.config(text="GO", command=go)
    cancel_event.clear()


#        sys.exit(0)

#    def force_shutdown():
#        executor.terminate_workers()
#        root.destroy()
#        sys.exit(1)
#
#    def shudown_soon(future):
#        root.after(100, clean_shutdown())
#

def clean_shutdown():
    executor.shutdown(wait=True, cancel_futures=True)
    queue_listener.stop()
    root.after(0, root.destroy)


def on_close():
    if current_future is not None and not current_future.done():
        if messagebox.askyesno("Cancel Backup and Quit",
                               "The archive backup is still running. Cancel and exit anyway?",
                               icon="warning"):
            cancel_event.set()
            Thread(target=clean_shutdown, name="clean_shutdown", daemon=True).start()
        else:
            return
    else:
        cancel_event.set()
        Thread(target=clean_shutdown, name="clean_shutdown", daemon=True).start()


def poll_progress():
    """
    Poll the multiprocessing queue for non-log messages such as progress,
    results, cancellation, or errors.
    """
    try:
        msg = progress_queue.get_nowait()

        if msg.get("type") == "artifact_total":
            total_artifacts = msg.get("value", 100)
            progress_bar['maximum'] = total_artifacts

        if msg.get("type") == "artifact_completed":
            progress_bar['value'] += msg.get("value")

    except Empty:
        pass

    root.after(10, poll_progress)


def append_text(text: str):
    output_text.config(state="normal")
    output_text.insert("end", text + "\n")
    output_text.see("end")
    output_text.config(state="disabled")


def clear_output():
    output_text.config(state="normal")
    output_text.delete("1.0", "end")
    output_text.config(state="disabled")


def read_options_from_config():
    config_file = get_config_path()
    return read_config(config_file=config_file)

def write_options_to_config(config) -> None:
    config_file = get_config_path()
    write_config(config_file=config_file, config_data=config)


if __name__ == "__main__":
    freeze_support()
    load_frozen_certs()
    main()
