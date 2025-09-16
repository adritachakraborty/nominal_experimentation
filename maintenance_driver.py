import datetime
import itertools
import time
import traceback

import config
import connect_python
from maintenance import (
    ChargeBalanceCtrlCmd,
    ChargeLedCtrlCmd,
    ChargeOffCmd,
    ChargeOnCmd,
    DataPacket,
    MaintEETelemetryCmd,
    MaintHVTelemetryCmd,
    MaintLVTelemetryCmd,
    MaintSystemTelemetryCmd,
    MaintTestPayloadCmd,
    SerialData,
)
from maintenance.SerialCom import SerialReadTimeout
from maintenance.UnpackHVTelemData import UnpackHVTelemData
from maintenance.UnpackLVTelemData import UnpackLVTelemData
from maintenance.UnpackSystemTelemData import UnpackSystemTelemData
from utils import (
    bms_id_to_int,
    df_from_scalar_dict,
    flatten_dict,
    get_streaming_enabled,
    log_file_path,
    write_data,
)

color_dict = {
    "NO FAULT": "\x1b[32m",
    "FAULT": "\x1b[31m",
    0: "\x1b[32m",
    1: "\x1b[31m",
}
RESET = "\x1b[0m"


class MaintenanceCommandData:
    def __init__(self):
        self.data = [{} for _ in range(7)]

    def set_vmsc_in_control(self, vmsc: str):
        self.data[1]["VMSC In Control"] = vmsc.replace(" ", "")

    def set_operational_mode_command(self, mode: str):
        if mode == "MAINTENANCE":
            mode = "MAINT"

        self.data[1]["Operational Mode Command"] = mode

    def set_bat_a_contactor_closed(self, closed: bool):
        self.data[1]["Bat-A HV Battery Contactor open/closed Command"] = "CLOSED" if closed else "OPEN"

    def set_bat_b_contactor_closed(self, closed: bool):
        self.data[1]["Bat-B HV Battery Contactor open/closed Command"] = "CLOSED" if closed else "OPEN"

    def set_positive_charge_contactor_closed(self, closed: bool):
        self.data[1]["Positive Charge Contactor open/close command"] = "CLOSED" if closed else "OPEN"

    def set_negative_charge_contactor_closed(self, closed: bool):
        self.data[1]["Negative Charge Contactor open/close command"] = "CLOSED" if closed else "OPEN"

    def set_reset_all_faults(self, reset: bool):
        self.data[6]["Reset All Faults"] = "CLEAR" if reset else "NO ACTION"

    def get_data(self):
        return self.data


class ChargeOnCommandData:
    def __init__(self):
        self.data = 0

    def set_data(self, mode: str):
        match mode:
            case "No Action":
                self.data = 0
            case "Start Charge Bat A":
                self.data = 1
            case "Start Charge Bat B":
                self.data = 2
            case "Start Charge Both":
                self.data = 3
            case _:
                print(f"Unknown Charge On Command mode: {mode}", flush=True)

    def get_data(self):
        return self.data


class ChargeLedCommandData:
    def __init__(self):
        self.data = 0

    def set_data(self, mode: str):
        match mode:
            case "Off":
                self.data = 0
            case "On":
                self.data = 1
            case "Error":
                self.data = 2
            case _:
                print(f"Unknown Charge LED Command mode: {mode}", flush=True)

    def get_data(self):
        return self.data


class ChargeBalanceCommandData:
    def __init__(self):
        self.data = 0

    def set_data(self, mode: str):
        match mode:
            case "Off":
                self.data = 0
            case "On":
                self.data = 1
            case _:
                print(f"Unknown Charge Balance Command mode: {mode}", flush=True)

    def get_data(self):
        return self.data


def sleep_until_exact(t: datetime.datetime) -> None:
    while datetime.datetime.now() < t:
        pass


def send_cmd(client: connect_python.Client, maintenance_bus_port: str):
    commandType = client.get_value("maintBusCommandType")
    match commandType:
        case "Maintenance":
            send_maintenance_cmd(client, maintenance_bus_port)
        case "Charge On":
            send_charge_on_cmd(client, maintenance_bus_port)
        case "Charge Off":
            send_charge_off_cmd(client, maintenance_bus_port)
        case "Charge LED":
            send_charge_led_cmd(client, maintenance_bus_port)
        case "Charge Balance":
            send_charge_balance_cmd(client, maintenance_bus_port)
        case _:
            print(f"Unknown command type: {commandType}", flush=True)


def send_maintenance_cmd(client: connect_python.Client, maintenance_bus_port: str):
    if not is_maintenance_mode(client):
        print("Not in maintenance mode, skipping MAINTENANCE command send", flush=True)
        return

    maintenance_command = MaintenanceCommandData()

    maintenance_command.set_vmsc_in_control(client.get_value("vmsInControl"))
    maintenance_command.set_operational_mode_command(client.get_value("operationalMode"))
    maintenance_command.set_bat_a_contactor_closed(client.get_value("isBatAContactorClosed"))
    maintenance_command.set_bat_b_contactor_closed(client.get_value("isBatBContactorClosed"))
    maintenance_command.set_positive_charge_contactor_closed(client.get_value("isPositiveChargeContactorClosed"))
    maintenance_command.set_negative_charge_contactor_closed(client.get_value("isNegativeChargeContactorClosed"))
    maintenance_command.set_reset_all_faults(client.get_value("resetAllFaults"))

    destId = bms_id_to_int(client.get_value("bmsId"))
    MaintTestPayloadCmd(
        destID=destId,
        sourceID=DataPacket.MaintSrcDestId.MAINT_APP_ID,
        serialData=get_serial_data(maintenance_bus_port),
        command_overrides=maintenance_command.get_data(),
    ).Run(interactive=False)

    client.set_value("maintCommandLastUpdated", str(datetime.datetime.now()))


def send_charge_on_cmd(client: connect_python.Client, maintenance_bus_port: str):
    if not is_maintenance_mode(client):
        print("Not in maintenance mode, skipping CHARGE ON command send", flush=True)
        return

    cmdData = ChargeOnCommandData()
    cmdData.set_data(client.get_value("chargeOnCommandPayload"))

    destId = bms_id_to_int(client.get_value("bmsId"))
    ChargeOnCmd(
        destID=destId,
        sourceID=DataPacket.MaintSrcDestId.MAINT_CRG_APP_ID,
        cmdData=cmdData.get_data(),
        serialData=get_serial_data(maintenance_bus_port),
    ).Run(interactive=False)

    client.set_value("chargeOnCommandLastUpdated", str(datetime.datetime.now()))


def send_charge_off_cmd(client: connect_python.Client, maintenance_bus_port: str):
    if not is_maintenance_mode(client):
        print("Not in maintenance mode, skipping CHARGE OFF command send", flush=True)
        return

    destId = bms_id_to_int(client.get_value("bmsId"))
    ChargeOffCmd(
        destID=destId,
        sourceID=DataPacket.MaintSrcDestId.MAINT_CRG_APP_ID,
        serialData=get_serial_data(maintenance_bus_port),
    ).Run(interactive=False)

    client.set_value("chargeOffCommandLastUpdated", str(datetime.datetime.now()))


def send_charge_led_cmd(client: connect_python.Client, maintenance_bus_port: str):
    if not is_maintenance_mode(client):
        print("Not in maintenance mode, skipping CHARGE LED command send", flush=True)
        return

    cmdData = ChargeLedCommandData()
    cmdData.set_data(client.get_value("chargeLEDCommandPayload"))

    destId = bms_id_to_int(client.get_value("bmsId"))
    ChargeLedCtrlCmd(
        destID=destId,
        sourceID=DataPacket.MaintSrcDestId.MAINT_CRG_APP_ID,
        cmdData=cmdData.get_data(),
        serialData=get_serial_data(maintenance_bus_port),
    ).Run(interactive=False)

    client.set_value("chargeLEDCommandLastUpdated", str(datetime.datetime.now()))


def send_charge_balance_cmd(client: connect_python.Client, maintenance_bus_port: str):
    if not is_maintenance_mode(client):
        print("Not in maintenance mode, skipping CHARGE BALANCE command send", flush=True)
        return

    cmdData = ChargeBalanceCommandData()
    cmdData.set_data(client.get_value("chargeBalanceCommandPayload"))

    destId = bms_id_to_int(client.get_value("bmsId"))
    ChargeBalanceCtrlCmd(
        destID=destId,
        sourceID=DataPacket.MaintSrcDestId.MAINT_CRG_APP_ID,
        cmdData=cmdData.get_data(),
        serialData=get_serial_data(maintenance_bus_port),
    ).Run(interactive=False)

    client.set_value("chargeBalanceCommandLastUpdated", str(datetime.datetime.now()))


def send_maintenance_hv_telemetry_cmd(
    client: connect_python.Client,
    maintenance_bus_port: str,
):
    destId = bms_id_to_int(client.get_value("bmsId"))
    if (
        words := MaintHVTelemetryCmd(
            destID=destId,
            sourceID=DataPacket.MaintSrcDestId.MAINT_APP_ID,
            serialData=get_serial_data(maintenance_bus_port),
        ).Run(interactive=False)
    ) is not None:
        unpacked_data = UnpackHVTelemData(words)
        extract_chassis_resistance(client, unpacked_data)
        extract_cell_voltages(client, unpacked_data)
        dict = flatten_dict(unpacked_data)
        dict["timestamp"] = time.time_ns()
        write_data(
            path=log_file_hv,
            client=client,
            df=df_from_scalar_dict(dict),
            streaming_enabled=get_streaming_enabled(client),
            stream_id="hv_telemetry",
        )

        client.set_value("hv_telemetry_last_updated", str(datetime.datetime.now()))


def send_maintenance_ee_telemetry_cmd(
    client: connect_python.Client,
    maintenance_bus_port: str,
):
    destId = bms_id_to_int(client.get_value("bmsId"))
    if (
        unpacked_data := MaintEETelemetryCmd(
            destID=destId,
            sourceID=DataPacket.MaintSrcDestId.MAINT_APP_ID,
            serialData=get_serial_data(maintenance_bus_port),
        ).Run(interactive=False)
    ) is not None:
        bat_temp_table(client, unpacked_data)
        extract_bmic_faults(client, unpacked_data)
        extract_terminal_voltages(client, unpacked_data)
        bat_soc_summary(client, unpacked_data)
        cell_volts_and_temps(client, unpacked_data)
        up_down_currents(client, unpacked_data)
        dict = flatten_dict(unpacked_data)
        dict["timestamp"] = time.time_ns()
        write_data(
            path=log_file_ee,
            client=client,
            df=df_from_scalar_dict(dict),
            streaming_enabled=get_streaming_enabled(client),
            stream_id="ee_telemetry",
        )

        client.set_value("ee_telemetry_last_updated", str(datetime.datetime.now()))


def send_maintenance_lv_telemetry_cmd(
    client: connect_python.Client,
    maintenance_bus_port: str,
):
    destId = bms_id_to_int(client.get_value("bmsId"))
    if (
        words := MaintLVTelemetryCmd(
            destID=destId,
            sourceID=DataPacket.MaintSrcDestId.MAINT_APP_ID,
            serialData=get_serial_data(maintenance_bus_port),
        ).Run(interactive=False)
    ) is not None:
        unpacked_data = UnpackLVTelemData(words)
        lv_telemetry(client, unpacked_data)
        lv_load_voltage_and_current(client, unpacked_data)
        dict = flatten_dict(unpacked_data)
        dict["timestamp"] = time.time_ns()
        write_data(
            path=log_file_lv,
            client=client,
            df=df_from_scalar_dict(dict),
            streaming_enabled=get_streaming_enabled(client),
            stream_id="lv_telemetry",
        )

        client.set_value("lv_telemetry_last_updated", str(datetime.datetime.now()))


def send_maintenance_system_telemetry_cmd(
    client: connect_python.Client,
    maintenance_bus_port: str,
):
    destId = bms_id_to_int(client.get_value("bmsId"))
    if (
        words := MaintSystemTelemetryCmd(
            destID=destId,
            sourceID=DataPacket.MaintSrcDestId.MAINT_APP_ID,
            serialData=get_serial_data(maintenance_bus_port),
        ).Run(interactive=False)
    ) is not None:
        unpacked_data = UnpackSystemTelemData(words)
        process_system_metadata(client, unpacked_data)
        process_i2c_faults(client, unpacked_data)
        process_bsb_temperatures(client, unpacked_data)
        dict = flatten_dict(unpacked_data)
        dict["timestamp"] = time.time_ns()
        write_data(
            path=log_file_system,
            client=client,
            df=df_from_scalar_dict(dict),
            streaming_enabled=get_streaming_enabled(client),
            stream_id="system_telemetry",
        )

        client.set_value("system_telemetry_last_updated", str(datetime.datetime.now()))


def round_robin(items):
    return itertools.cycle(items)


cycler = round_robin(
    [
        send_cmd,
        send_maintenance_hv_telemetry_cmd,
        send_maintenance_lv_telemetry_cmd,
        send_maintenance_ee_telemetry_cmd,
        send_maintenance_system_telemetry_cmd,
    ]
)


def send_maintenance(client: connect_python.Client, maintenance_bus_port: str):
    try:
        cmd = next(cycler)
        cmd(client, maintenance_bus_port)
    except SerialReadTimeout as e:
        print("Serial read timeout", e, flush=True)
    except Exception as e:
        print(f"Error communicating on maintenance bus: {e}", flush=True)
        traceback.print_exc()
        return None


serialData = None


def get_serial_data(maintenance_bus_port):
    global serialData
    if serialData is None:
        serialData = SerialData.SerialDataCls(maintenance_bus_port)
    return serialData


def bat_temp_table(client: connect_python.Client, values: dict):
    cols = ["Battery", "Module", "Min Temperature", "Max Temperature", "Min Voltage", "Max Voltage", "Alert"]

    is_charging = client.get_value("hvpmu_positive_charge_contactor") and client.get_value(
        "hvpmu_negative_charge_contactor"
    )

    def format_row(temperatures, voltages):
        min_temp = min(temperatures)
        max_temp = max(temperatures)
        min_volt = min(voltages)
        max_volt = max(voltages)

        alert = min_volt < 2.75 or max_volt > 4.2 or max_temp > 55 or (is_charging and max_temp > 40)

        return [f"{min_temp:.2f}", f"{max_temp:.2f}", f"{min_volt:.2f}", f"{max_volt:.2f}", "■" if alert else ""]

    rows = [
        [
            bat if mod == 1 else "",
            str(mod),
            *format_row(
                [
                    values[f"BAT {bat} CELL TEMPERATURE"][f"Bat {bat} Module {mod} Cell Temp {cell}"]
                    for cell in range(1, 7)
                ],
                [
                    values[f"BAT {bat} CELL VOLTAGE"][f"Bat {bat} Module {mod} Cell {cell} Voltage"]
                    for cell in range(1, 31)
                ],
            ),
        ]
        for bat in ["A", "B"]
        for mod in range(1, 7)
    ]

    client.set_value(
        "battery_cells_table",
        {
            "columns": cols,
            "data": list(zip(*rows)),
        },
    )


def extract_cell_voltages(client: connect_python.Client, values: dict):
    bat_a_data = values["Battery A Data"]
    bat_b_data = values["Battery B Data"]

    client.set_value("maint_bat_a_max_cell_voltage", bat_a_data["Bat-A Max Cell Voltage"])
    client.set_value("maint_bat_a_min_cell_voltage", bat_a_data["Bat-A Min Cell Voltage"])
    client.set_value(
        "maint_bat_a_delta_cell_voltage", bat_a_data["Bat-A Max Cell Voltage"] - bat_a_data["Bat-A Min Cell Voltage"]
    )

    client.set_value("maint_bat_b_max_cell_voltage", bat_b_data["Bat-B Max Cell Voltage"])
    client.set_value("maint_bat_b_min_cell_voltage", bat_b_data["Bat-B Min Cell Voltage"])
    client.set_value(
        "maint_bat_b_delta_cell_voltage", bat_b_data["Bat-B Max Cell Voltage"] - bat_b_data["Bat-B Min Cell Voltage"]
    )


def extract_terminal_voltages(client: connect_python.Client, values: dict):
    client.set_value("maint_bat_a_hv_terminal_voltage", values["Bat-A HV Terminal Voltage"])
    client.set_value("maint_bat_b_hv_terminal_voltage", values["Bat-B HV Terminal Voltage"])


def extract_bmic_faults(client: connect_python.Client, values: dict):
    if "BAT A BMIC FAULT" not in values and "BAT B BMIC FAULT" not in values:
        return

    maint_bmic_fault = values.get("BMIC Fault", "No Fault")
    client.set_value("maint_bmic_fault", maint_bmic_fault)

    if "BAT A BMIC FAULT" in values:
        for module in range(1, 7):
            module_faults = []
            for bmic in range(1, 4):
                fault_status = values["BAT A BMIC FAULT"][f"Bat-A Module {module} BMIC {bmic} Fault"]
                module_faults.append(f"{fault_status}")
            client.set_value(f"maint_bat_a_mod_{module}_bmic_faults", str(module_faults))

    if "BAT B BMIC FAULT" in values:
        for module in range(1, 7):
            module_faults = []
            for bmic in range(1, 4):
                fault_status = values["BAT B BMIC FAULT"][f"Bat-B Module {module} BMIC {bmic} Fault"]
                module_faults.append(f"{fault_status}")
            client.set_value(f"maint_bat_b_mod_{module}_bmic_faults", str(module_faults))


def cell_volts_and_temps(client: connect_python.Client, values: dict):
    if "BAT A CELL VOLTAGE" in values:
        for module in range(1, 7):
            for cell in range(1, 31):
                client.set_value(
                    f"bat_a_mod_{module}_cell_volt_{cell}",
                    f"{values['BAT A CELL VOLTAGE'][f'Bat A Module {module} Cell {cell} Voltage']:.0f}",
                )
            for cell in range(1, 7):
                client.set_value(
                    f"bat_a_mod_{module}_cell_temp_{cell}",
                    f"{values['BAT A CELL TEMPERATURE'][f'Bat A Module {module} Cell Temp {cell}']:.0f}",
                )
    if "BAT B CELL VOLTAGE" in values:
        for module in range(1, 7):
            for cell in range(1, 31):
                client.set_value(
                    f"bat_b_mod_{module}_cell_volt_{cell}",
                    f"{values['BAT B CELL VOLTAGE'][f'Bat B Module {module} Cell {cell} Voltage']:.0f}",
                )
            for cell in range(1, 7):
                client.set_value(
                    f"bat_b_mod_{module}_cell_temp_{cell}",
                    f"{values['BAT B CELL TEMPERATURE'][f'Bat B Module {module} Cell Temp {cell}']:.0f}",
                )


def bat_soc_summary(client: connect_python.Client, values: dict):
    def calculate_soc_metrics(bat_letter):
        if f"BAT {bat_letter} CELL SOC" not in values:
            return None, None, None

        soc_data = values[f"BAT {bat_letter} CELL SOC"]
        soc_values = []

        for module in range(1, 7):
            for cell in range(1, 31):
                key = f"Bat {bat_letter} Module {module} Cell {cell} SOC"
                if key in soc_data:
                    soc_values.append(soc_data[key])

        if not soc_values:
            return None, None, None

        min_soc = min(soc_values)
        max_soc = max(soc_values)
        delta_soc = max_soc - min_soc

        return min_soc, max_soc, delta_soc

    bat_a_min, bat_a_max, bat_a_delta = calculate_soc_metrics("A")
    bat_b_min, bat_b_max, bat_b_delta = calculate_soc_metrics("B")

    if bat_a_min is not None:
        client.set_value("maint_bat_a_min_cell_soc", f"\t{bat_a_min:.1f}")
        client.set_value("maint_bat_a_max_cell_soc", f"\t{bat_a_max:.1f}")
        if bat_a_delta > 5.0:
            DISPLAY_COLOR = color_dict[1]
        else:
            DISPLAY_COLOR = color_dict[0]
        client.set_value("maint_bat_a_delta_cell_soc", f"\t{DISPLAY_COLOR}{bat_a_delta:.1f}{RESET}")
    if bat_b_min is not None:
        client.set_value("maint_bat_b_min_cell_soc", f"\t{bat_b_min:.1f}")
        client.set_value("maint_bat_b_max_cell_soc", f"\t{bat_b_max:.1f}")
        if bat_b_delta > 5.0:
            DISPLAY_COLOR = color_dict[1]
        else:
            DISPLAY_COLOR = color_dict[0]
        client.set_value("maint_bat_b_delta_cell_soc", f"\t{DISPLAY_COLOR}{bat_b_delta:.1f}{RESET}")


def extract_chassis_resistance(client: connect_python.Client, values: dict):
    if "Isolation Resistance Raw Values" not in values:
        return

    iso_res_data = values["Isolation Resistance Raw Values"]
    for i in range(1, 3):
        client.set_value(f"bat_{i}_hv_chassis_resistance_pos", iso_res_data[f"Bat {i} HV + to Chassis Resistance"])
        client.set_value(f"bat_{i}_hv_chassis_resistance_neg", iso_res_data[f"Bat {i} HV - to Chassis Resistance"])


def up_down_currents(client: connect_python.Client, values: dict):
    if "BAT A DC/DC UPSTREAM CURRENT" in values:
        for i in range(1, 7):
            upstream_value = values["BAT A DC/DC UPSTREAM CURRENT"][f"Bat 1 Module {i} DC/DC Upstream Current"]
            client.set_value(f"maint_bat_1_upstream_current_{i}", upstream_value)

    if "BAT B DC/DC UPSTREAM CURRENT" in values:
        for i in range(1, 7):
            upstream_value = values["BAT B DC/DC UPSTREAM CURRENT"][f"Bat 2 Module {i} DC/DC Upstream Current"]
            client.set_value(f"maint_bat_2_upstream_current_{i}", upstream_value)


def lv_telemetry(client: connect_python.Client, values: dict):
    lvpdu_data = values.get("LVPDU Data", None)
    if not lvpdu_data:
        return

    client.set_value("maint_bat_a_lvpdu_output_voltage", lvpdu_data["Bat-A LVPDU Output Voltage"])
    client.set_value("maint_bat_a_lvpdu_output_current", lvpdu_data["Bat-A LVPDU Output Current"])
    client.set_value("maint_bat_a_lvpdu_voltage_sensor_error", lvpdu_data["Bat-A LVPDU Voltage Sensor Error"])
    client.set_value("maint_bat_a_lvpdu_current_sensor_error", lvpdu_data["Bat-A LVPDU Current Sensor Error"])
    client.set_value("maint_lock_lv_load_echo", lvpdu_data["Lock LV Load Echo"])

    client.set_value("maint_bat_b_lvpdu_output_voltage", lvpdu_data["Bat-B LVPDU Output Voltage"])
    client.set_value("maint_bat_b_lvpdu_output_current", lvpdu_data["Bat-B LVPDU Output Current"])
    client.set_value("maint_bat_b_lvpdu_voltage_sensor_error", lvpdu_data["Bat-B LVPDU Voltage Sensor Error"])
    client.set_value("maint_bat_b_lvpdu_current_sensor_error", lvpdu_data["Bat-B LVPDU Current Sensor Error"])


def lv_load_voltage_and_current(client: connect_python.Client, values: dict):
    rows = [
        [
            f"LV Load {i}",
            "■" if values["LV Load Feedback"][f"LV Load {i} Feedback"] == "ON" else "",
            f"{values['LV Load Currents'][f'LVPDU Load {i} Current']:.2f}",
            f"{values['LV Load Voltages'][f'LVPDU Load {i} Voltage']:.2f}",
        ]
        for i in range(1, 31)
    ]
    client.set_value(
        "lv_load_telemetry",
        {
            "columns": [
                "",
                "LV Load Feedback",
                "LV Load Current",
                "LV Load Voltage",
            ],
            "data": list(zip(*rows)),
        },
    )


def process_system_metadata(client: connect_python.Client, values: dict):
    client.set_value("bms_part_number", values["BMS Part Number"])
    client.set_value("bms_serial_number", values["BMS Serial Number"])
    client.set_value("firmware_part_number", values["Firmware Part Number"])
    client.set_value("bms_mode_status", values["BMS Mode Status"])


def process_i2c_faults(client: connect_python.Client, values: dict):
    if "Bat A Module 1 I2C Fault" not in values and "Bat B Module 1 I2C Fault" not in values:
        return

    client.set_value("cda_output_sensor", values["CDA Output Sensor"])
    client.set_value("data_pdi_crc_fault", values["Data PDI CRC Fault"])
    client.set_value("config_pdi_crc_fault", values["Config PDI CRC Fault"])

    if "Bat A Module 1 I2C Fault" in values:
        for module in range(1, 7):
            fault_value = values[f"Bat A Module {module} I2C Fault"]
            client.set_value(f"bat_a_mod_{module}_i2c_fault", f"{color_dict[fault_value]}{fault_value}{RESET}")
    if "Bat B Module 1 I2C Fault" in values:
        for module in range(1, 7):
            fault_value = values[f"Bat B Module {module} I2C Fault"]
            client.set_value(f"bat_b_mod_{module}_i2c_fault", f"{color_dict[fault_value]}{fault_value}{RESET}")


def process_bsb_temperatures(client: connect_python.Client, values: dict):
    if "Battery A Module 1 BSB Temp" not in values and "Battery B Module 1 BSB Temp" not in values:
        return

    if "Battery A Module 1 BSB Temp" in values:
        for module in range(1, 7):
            client.set_value(f"bat_a_mod_{module}_bsb_temp", values[f"Battery A Module {module} BSB Temp"])
    if "Battery B Module 1 BSB Temp" in values:
        for module in range(1, 7):
            client.set_value(f"bat_b_mod_{module}_bsb_temp", values[f"Battery B Module {module} BSB Temp"])


def is_maintenance_mode(client: connect_python.Client):
    return client.get_value("operationalMode") == "MAINTENANCE"


t = 0


def run(client: connect_python.Client, maintenance_bus_port: str):
    global t

    global log_file_hv
    global log_file_lv
    global log_file_ee
    global log_file_system
    logs_folder = "./logs"
    log_file_hv = log_file_path(logs_folder, "hv_telemetry")
    log_file_lv = log_file_path(logs_folder, "lv_telemetry")
    log_file_ee = log_file_path(logs_folder, "ee_telemetry")
    log_file_system = log_file_path(logs_folder, "system_telemetry")

    # Cell Telemetry details
    for bat in ["a", "b"]:
        for module in range(1, 7):
            for n in range(1, 7):
                client.clear_stream(f"bat_{bat}_mod_{module}_cell_temp_{n}")

    test_start = datetime.datetime.now()
    while True:
        cycle_start = datetime.datetime.now()
        send_maintenance(client, maintenance_bus_port)
        t = cycle_start.timestamp() - test_start.timestamp()

        client.set_value("maintenanceModeEnabled", is_maintenance_mode(client))

        # Sleep to maintain a 1Hz cycle
        sleep_until_exact(cycle_start + datetime.timedelta(seconds=1))


@connect_python.main
def main(client: connect_python.Client):
    maintenance_bus_port = client.get_value("maintenanceBusPort")

    client.set_value("dataset_rid", config.dataset_rid)
    try:
        run(client, maintenance_bus_port)
    finally:
        pass


if __name__ == "__main__":
    main()
