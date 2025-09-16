import datetime
import time

import config
import connect_python
import nifpga
from utils import (
    bms_id_to_dec,
    df_from_scalar_dict,
    get_streaming_enabled,
    log_file_path,
    vmsc_id_to_dec,
    write_data,
)

logger = connect_python.get_logger(__name__)


class Channel:
    """Channel class to represent a channel in the FPGA."""

    def __init__(self, session: nifpga.Session, channel_num: int):
        """Args:
        session (nifpga.Session): The FPGA session.
        channel_num (int): The channel number (1 or 2).
        """
        self.session = session
        self.channel_num = channel_num

    def size_register(self):
        """Write the size of data to be transmitted to BMSC using this register"""
        register_name = f"Ch{self.channel_num} Size"
        return self.session.registers[register_name]

    def data_register(self):
        """Write the data to be transmitted to the BMSC using this register"""
        register_name = f"Ch{self.channel_num} Data"
        return self.session.registers[register_name]

    def frames_received_register(self):
        """Maintains the number of frames received"""
        if self.channel_num == 1:
            register_name = f"Ch{self.channel_num} Frames_Recvd "  # trailing space is intentional
        else:
            register_name = f"Ch{self.channel_num} Frames_Recvd"
        return self.session.registers[register_name]

    def send_register(self):
        """Set this register to true to transmit data and back to false to receive data"""
        register_name = f"Ch{self.channel_num} Send"
        return self.session.registers[register_name]

    def frame_register(self):
        """The latest frame received"""
        if self.channel_num == 1:
            register_name = f"Ch{self.channel_num} Frame "  # trailing space is intentional
        else:
            register_name = f"Ch{self.channel_num} Frame"
        return self.session.registers[register_name]

    def bytes_received_register(self):
        """The number of bytes in the latest frame received"""
        if self.channel_num == 1:
            register_name = f"Ch{self.channel_num} Bytes_Recvd "  # trailing space is intentional
        else:
            register_name = f"Ch{self.channel_num} Bytes_Recvd"
        return self.session.registers[register_name]


def crc32_non_reversed(data):
    """CRC-32 implementation using non-reversed polynomial (0x04C11DB7)"""
    crc = 0xFFFFFFFF
    polynomial = 0x04C11DB7  # This is the non-reversed polynomial

    for byte in data:
        crc ^= byte << 24
        for _ in range(8):
            if crc & 0x80000000:
                crc = (crc << 1) ^ polynomial
            else:
                crc = crc << 1

    return ~crc & 0xFFFFFFFF  # Final XOR and ensure 32-bit unsigned


class CommandData:
    def __init__(self):
        self.data = [0, 64, 0, 0, 0, 0, 0, 0, 16, 80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        # Pad to 256 bytes expected by the register
        while len(self.data) < 256:
            self.data.append(0)

    def set_bms_id(self, bms_id: str):
        bms_id_dec = bms_id_to_dec(bms_id)
        if bms_id_dec is not None:
            self.data[2] = bms_id_dec
        else:
            raise ValueError(f"Invalid BMS ID: {bms_id}")

    def set_vmsc_id(self, vmsc_id: str):
        vmsc_id_dec = vmsc_id_to_dec(vmsc_id)
        if vmsc_id_dec is not None:
            self.data[3] = vmsc_id_dec
        else:
            raise ValueError(f"Invalid VMSC ID: {vmsc_id}")

    def set_sequence_num(self, sequence_num):
        # Split the sequence number into individual bytes
        high_byte = (sequence_num >> 8) & 0xFF
        low_byte = sequence_num & 0xFF

        # Set the sequence number in the payload
        self.data[6] = low_byte
        self.data[7] = high_byte

    def set_vmsc_in_control(self, vmsc_in_control_index: int):
        byte = self.data[14]
        byte = byte & 0b11111100 | (vmsc_in_control_index << 0)
        self.data[14] = byte

    def set_operational_mode(self, operational_mode: str):
        mode_to_flag = {
            "OFF": 000,
            "STANDBY": 0b001,
            "PREFLIGHT": 0b10,
            "ARMED": 0b011,
            "FLIGHT": 0b100,
            "POSTFLIGHT": 0b101,
            "MAINTENANCE": 0b110,
        }
        flag = mode_to_flag[operational_mode]
        byte = self.data[14]
        byte = byte & 0b11100011 | (flag << 2)
        self.data[14] = byte

    def set_clear_inflight_bootup_condition(self, clear: bool):
        flag = 1 if clear else 0
        byte = self.data[14]
        byte = byte & 0b11011111 | (flag << 5)
        self.data[14] = byte

    def set_bat_a_breaktor_closed(self, closed: bool):
        flag = 1 if closed else 0
        byte = self.data[14]
        byte = byte & 0b10111111 | (flag << 6)
        self.data[14] = byte

    def set_bat_b_breaktor_closed(self, closed: bool):
        flag = 1 if closed else 0
        byte = self.data[14]
        byte = byte & 0b01111111 | (flag << 7)
        self.data[14] = byte

    def set_lv_load(self, load_number: int, enable: bool):
        if not 1 <= load_number <= 30:
            raise ValueError(f"LV Load number must be between 1 and 30, got {load_number}")

        # Adjust load_number to be 0-indexed
        load_index = load_number - 1

        # Calculate which byte and bit position to modify
        byte_index = 22 + (load_index // 8)
        bit_position = load_index % 8

        # Set or clear the bit
        if enable:
            self.data[byte_index] |= 1 << bit_position
        else:
            self.data[byte_index] &= ~(1 << bit_position)

    def get_data(self):
        self._update_crc()
        return self.data

    def _update_crc(self):
        # Calculate CRC32 on the first 26 bytes (all except the last 4)
        crc_value = crc32_non_reversed(bytes(self.data[0:26]))

        # Convert the CRC32 value to 4 bytes (little-endian)
        crc_bytes = [
            crc_value & 0xFF,  # Least significant byte
            (crc_value >> 8) & 0xFF,
            (crc_value >> 16) & 0xFF,
            (crc_value >> 24) & 0xFF,  # Most significant byte
        ]

        # Insert the CRC bytes into the last 4 positions
        self.data[26:30] = crc_bytes


# TODO: Validate decoded response data
class ResponseData:
    def __init__(self, data: list[int] | None = None):
        self.data = data if data is not None else []
        while len(self.data) < 94:
            self.data.append(0)

    @property
    def payload(self):
        return self.data[10:-4]

    # Word 0: BMS Mode Echo & General
    def vmsc_in_control_echo(self):
        return self.payload[0] & 0b00000011

    def vmsc1_in_control_echo(self):
        return (self.payload[0] & 0b00001100) >> 2

    def vmsc2_in_control_echo(self):
        return (self.payload[0] & 0b00110000) >> 4

    def vmsc3_in_control_echo(self):
        return (self.payload[0] & 0b11000000) >> 6

    def operational_mode_echo(self):
        return self.payload[1] & 0b00000111

    def clear_inflight_bootup_conditions_echo(self):
        return bool((self.payload[1] & 0b00001000) >> 3)

    def bat_a_hv_battery_contactor_open_closed_echo(self):
        return bool((self.payload[1] & 0b00010000) >> 4)

    def bat_b_hv_battery_contactor_open_closed_echo(self):
        return bool((self.payload[1] & 0b00100000) >> 5)

    def hv_aux_contactor_open_closed_echo(self):
        return bool((self.payload[1] & 0b01000000) >> 6)

    def bms_in_control_of_battery_contactors_echo(self):
        return bool((self.payload[1] & 0b10000000) >> 7)

    def enable_cell_balancing_echo(self):
        return bool(self.payload[2] & 0b00000001)

    def psp_master_switch(self):
        return (self.payload[2] & 0b00011000) >> 3

    def psp_lockout_switch(self):
        return (self.payload[2] & 0b01100000) >> 5

    def psp_maintenance_switch(self):
        return (self.payload[2] & 0b10000000) >> 7 | ((self.payload[3] & 0b00000001) << 1)

    def lock_hv_load_echo(self):
        return bool((self.payload[3] & 0b00000010) >> 1)

    def lock_lv_load_echo(self):
        return bool((self.payload[3] & 0b00000100) >> 2)

    # Word 1: BMS Modes & General
    def vmsc1_in_control_status(self):
        return bool(self.payload[4] & 0b00000001)

    def vmsc2_in_control_status(self):
        return bool((self.payload[4] & 0b00000010) >> 1)

    def vmsc3_in_control_status(self):
        return bool((self.payload[4] & 0b00000100) >> 2)

    def operational_mode_status(self):
        return int((self.payload[4] & 0b00111000) >> 3)

    def inflight_bootup(self):
        return bool((self.payload[4] & 0b01000000) >> 6)

    def psp_hv_light_status(self):
        return (self.payload[4] & 0b10000000) >> 7 | ((self.payload[5] & 0b00000001) << 1)

    def psp_lv_light_status(self):
        return (self.payload[5] & 0b00000110) >> 1

    def psp_charge_light_status(self):
        return (self.payload[5] & 0b00011000) >> 3

    def psp_maintenance_light_status(self):
        return (self.payload[5] & 0b01100000) >> 5

    def charge_cable_mated(self):
        return bool((self.payload[5] & 0b10000000) >> 7)

    def bat_a_hv_battery_contactor_open_closed_status(self):
        return bool(self.payload[6] & 0b00000001)

    def bat_b_hv_battery_contactor_open_closed_status(self):
        return bool((self.payload[6] & 0b00000010) >> 1)

    def hv_aux_contactor_open_closed_status(self):
        return bool((self.payload[6] & 0b00000100) >> 2)

    def hvpmu_positive_charge_contactor(self):
        return bool((self.payload[6] & 0b00001000) >> 3)

    def hvpmu_negative_charge_contactor(self):
        return bool((self.payload[6] & 0b00010000) >> 4)

    def hvpmu_soft_short_detected(self):
        return bool((self.payload[6] & 0b00100000) >> 5)

    def hvpmu_hard_short_detected(self):
        return bool((self.payload[6] & 0b01000000) >> 6)

    def bms_in_control_of_battery_contactors_status(self):
        return bool((self.payload[6] & 0b10000000) >> 7)

    def enable_cell_balancing_status(self):
        return bool(self.payload[7] & 0b00000001)

    def maintenance_bus_mated(self):
        return bool((self.payload[7] & 0b00000010) >> 1)

    # Word 2: BAT A Status 1
    def bat_a_hv_terminal_voltage(self):
        return (self.payload[8] | ((self.payload[9] & 0b00000011) << 8)) * 800 / 1023

    def bat_a_average_cell_temp(self):
        precision = (self.payload[9] & 0b11111100) >> 2 | ((self.payload[10] & 0b00000011) << 6)
        return float(precision) * 120 / 255

    def input_current_mismatch(self):
        return bool((self.payload[10] & 0b00000100) >> 2)

    # Word 3: BAT A Status 2
    def bat_a_lvpdu_output_voltage(self):
        precision = self.payload[12] | ((self.payload[13] & 0b00000011) << 8)
        return float(precision) * 33 / 1023

    def bat_a_lvpdu_output_current(self):
        precision = (self.payload[13] & 0b11111100) >> 2 | ((self.payload[14] & 0b00001111) << 6)
        return float(precision) * 90 / 1023

    def bat_a_battery_charging_status(self):
        return bool((self.payload[14] & 0b00010000) >> 4)

    def bat_a_hv_terminal_voltage_sensor_error(self):
        return bool((self.payload[14] & 0b00100000) >> 5)

    def bat_a_hv_current_sensor_error(self):
        return bool((self.payload[14] & 0b01000000) >> 6)

    def bat_a_hv_temperature_sensor_error(self):
        return bool((self.payload[14] & 0b10000000) >> 7)

    def bat_a_lvpdu_voltage_sensor_error(self):
        return bool(self.payload[15] & 0b00000001)

    def bat_a_lvpdu_current_sensor_error(self):
        return bool((self.payload[15] & 0b00000010) >> 1)

    def bat_a_hv_remaining_energy_sensor_error(self):
        return bool((self.payload[15] & 0b00000100) >> 2)

    def bat_a_state_of_charge_sensor_error(self):
        return bool((self.payload[15] & 0b00001000) >> 3)

    def bat_a_state_of_health_sensor_error(self):
        return bool((self.payload[15] & 0b00010000) >> 4)

    def bat_a_charge_capacity_sensor_error(self):
        return bool((self.payload[15] & 0b00100000) >> 5)

    def bat_a_gas_sensor_error(self):
        return bool((self.payload[15] & 0b01000000) >> 6)

    # Word 4: BAT A Status 3
    def bat_a_time_to_charged(self):
        return self.payload[16] | ((self.payload[17] & 0b00000011) << 8)

    def bat_a_hv_remaining_energy(self):
        return ((self.payload[17] & 0b11111100) >> 2) * 15 / 255

    def bat_a_target_state_of_charge(self):
        return (self.payload[18] | ((self.payload[19] & 0b00000011) << 8)) * 100 / 1023

    # Word 5: BAT A Status 4
    def bat_a_state_of_charge(self):
        return (self.payload[20] | ((self.payload[21] & 0b00000011) << 8)) * 100 / 1023

    def bat_a_state_of_health(self):
        return ((self.payload[21] & 0b11111100) >> 2 | ((self.payload[22] & 0b00000011) << 6)) * 20 / 1023 + 80

    def bat_a_charge_capacity(self):
        return ((self.payload[22] & 0b11111100) >> 2 | ((self.payload[23] & 0b00000011) << 6)) * 5 / 1023 + 15

    # Word 6: BAT B Status 1
    def bat_b_hv_terminal_voltage(self):
        return (self.payload[24] | ((self.payload[25] & 0b00000011) << 8)) * 800 / 1023

    def bat_b_average_cell_temp(self):
        return ((self.payload[25] & 0b11111100) >> 2) * 120 / 255

    # Word 7: BAT B Status 2
    def bat_b_lvpdu_output_voltage(self):
        precision = self.payload[28] | ((self.payload[29] & 0b00000011) << 8)
        return float(precision) * 33 / 1023

    def bat_b_lvpdu_output_current(self):
        precision = (self.payload[29] & 0b11111100) >> 2 | ((self.payload[30] & 0b00001111) << 6)
        return float(precision) * 90 / 1023

    def bat_b_battery_charging_status(self):
        return bool((self.payload[30] & 0b00010000) >> 4)

    def bat_b_hv_terminal_voltage_sensor_error(self):
        return bool((self.payload[30] & 0b00100000) >> 5)

    def bat_b_hv_current_sensor_error(self):
        return bool((self.payload[30] & 0b01000000) >> 6)

    def bat_b_hv_temperature_sensor_error(self):
        return bool((self.payload[30] & 0b10000000) >> 7)

    def bat_b_lvpdu_voltage_sensor_error(self):
        return bool(self.payload[31] & 0b00000001)

    def bat_b_lvpdu_current_sensor_error(self):
        return bool((self.payload[31] & 0b00000010) >> 1)

    def bat_b_hv_remaining_energy_sensor_error(self):
        return bool((self.payload[31] & 0b00000100) >> 2)

    def bat_b_state_of_charge_sensor_error(self):
        return bool((self.payload[31] & 0b00001000) >> 3)

    def bat_b_state_of_health_sensor_error(self):
        return bool((self.payload[31] & 0b00010000) >> 4)

    def bat_b_charge_capacity_sensor_error(self):
        return bool((self.payload[31] & 0b00100000) >> 5)

    def bat_b_gas_sensor_error(self):
        return bool((self.payload[31] & 0b01000000) >> 6)

    # Word 8: BAT B Status 3
    def bat_b_time_to_charged(self):
        return self.payload[32] | ((self.payload[33] & 0b00000011) << 8)

    def bat_b_hv_remaining_energy(self):
        return ((self.payload[33] & 0b11111100) >> 2) * 15 / 255

    def bat_b_target_state_of_charge(self):
        return (self.payload[34] | ((self.payload[35] & 0b00000011) << 8)) * 100 / 1023

    # Word 9: BAT B Status 4
    def bat_b_state_of_charge(self):
        return (self.payload[36] | ((self.payload[37] & 0b00000011) << 8)) * 100 / 1023

    def bat_b_state_of_health(self):
        return ((self.payload[37] & 0b11111100) >> 2 | ((self.payload[38] & 0b00000011) << 6)) * 20 / 1023 + 80

    def bat_b_charge_capacity(self):
        return ((self.payload[38] & 0b11111100) >> 2 | ((self.payload[39] & 0b00000011) << 6)) * 5 / 1023 + 15

    # Word 10: Fault Messages
    def bat_a_hv_battery_contactor_mismatch(self):
        return bool(self.payload[40] & 0b00000001)

    def bat_b_hv_battery_contactor_mismatch(self):
        return bool((self.payload[40] & 0b00000010) >> 1)

    def hv_aux_contactor_mismatch(self):
        return bool((self.payload[40] & 0b00000100) >> 2)

    def hvpmu_positive_charge_contactor_mismatch(self):
        return bool((self.payload[40] & 0b00001000) >> 3)

    def hvpmu_negative_charge_contactor_mismatch(self):
        return bool((self.payload[40] & 0b00010000) >> 4)

    def bat_a_power_isolation_fault(self):
        return bool((self.payload[40] & 0b00100000) >> 5)

    def bat_b_power_isolation_fault(self):
        return bool((self.payload[40] & 0b01000000) >> 6)

    def bat_a_return_isolation_fault(self):
        return bool((self.payload[40] & 0b10000000) >> 7)

    def bat_b_return_isolation_fault(self):
        return bool(self.payload[41] & 0b00000001)

    def pack_to_pack_isolation_fault(self):
        return bool((self.payload[41] & 0b00000010) >> 1)

    def thermal_runaway_fault(self):
        return bool((self.payload[41] & 0b00000100) >> 2)

    def bmic_fault(self):
        return bool((self.payload[41] & 0b00001000) >> 3)

    def dcp_fault(self):
        return bool((self.payload[41] & 0b00010000) >> 4)

    def cbit_rollup(self):
        return bool((self.payload[41] & 0b01000000) >> 6)

    # Word 11: HVPMU Output Current
    def hvpmu_output_1_current(self):
        precision = self.payload[44] | ((self.payload[45] & 0b00000011) << 8)
        return float(precision) * 850 / 1023 - 100

    def hvpmu_output_2_current(self):
        precision = ((self.payload[45] >> 2) & 0b00111111) | ((self.payload[46] & 0b00001111) << 6)
        return float(precision) * 850 / 1023 - 100

    def hvpmu_aux_output_current(self):
        precision = ((self.payload[46] & 0b11110000) >> 4) | ((self.payload[47] & 0b00111111) << 4)
        return float(precision) * 850 / 1023 - 100

    # Word 12: LV Loads Feedback
    def lv_load_feedback(self, load_number):
        if not 1 <= load_number <= 30:
            raise ValueError("Load number must be between 1 and 30")

        word_offset = 48
        bit_offset = load_number - 1
        byte_index = word_offset + (bit_offset // 8)
        bit_in_byte = bit_offset % 8

        return bool((self.payload[byte_index] & (1 << bit_in_byte)) >> bit_in_byte)

    # Word 13: LV Loads Overcurrent
    def lv_load_overcurrent(self, load_number):
        if not 1 <= load_number <= 30:
            raise ValueError("Load number must be between 1 and 30")

        word_offset = 52
        bit_offset = load_number - 1
        byte_index = word_offset + (bit_offset // 8)
        bit_in_byte = bit_offset % 8

        return bool((self.payload[byte_index] & (1 << bit_in_byte)) >> bit_in_byte)

    # Word 14: BMS Identification
    def bms_part_number(self):
        # Extract 2 ASCII characters
        return chr(self.payload[56]) + chr(self.payload[57])

    def bms_serial_number(self):
        return self.payload[58] | (self.payload[59] << 8)

    # Word 15: Firmware Identification
    def firmware_part_number(self):
        # Extract 4 ASCII characters
        return chr(self.payload[60]) + chr(self.payload[61]) + chr(self.payload[62]) + chr(self.payload[63])

    # Word 16: TELEMETRY 1 Output Current
    def bat_a_output_current(self):
        value = self.payload[64] | (self.payload[65] << 8)
        # Handle signed 16-bit integer
        if value & 0x8000:  # Check if sign bit is set
            value = value - 0x10000  # Convert to negative
        return value * 450 / 32767  # 2^15-1 = 32767

    def bat_b_output_current(self):
        value = self.payload[66] | (self.payload[67] << 8)
        # Handle signed 16-bit integer
        if value & 0x8000:  # Check if sign bit is set
            value = value - 0x10000  # Convert to negative
        return value * 450 / 32767  # 2^15-1 = 32767

    # Word 17: TELEMETRY 2 Output Voltage
    def hvpmu_output_voltage(self):
        return (self.payload[68] | ((self.payload[69] & 0b00000011) << 8)) * 800 / 1023

    # Word 18: TELEMETRY 3 Max/Min Voltage
    def bat_a_max_cell_voltage(self):
        return self.payload[72] * 4.3 / 255

    def bat_b_max_cell_voltage(self):
        return self.payload[73] * 4.3 / 255

    def bat_a_min_cell_voltage(self):
        return self.payload[74] * 4.3 / 255

    def bat_b_min_cell_voltage(self):
        return self.payload[75] * 4.3 / 255

    # Word 19: TELEMETRY 4 Max/Min Temperature
    def bat_a_max_cell_temperature(self):
        return self.payload[76] * 120 / 255

    def bat_b_max_cell_temperature(self):
        return self.payload[77] * 120 / 255

    def bat_a_min_cell_temperature(self):
        return self.payload[78] * 120 / 255

    def bat_b_min_cell_temperature(self):
        return self.payload[79] * 120 / 255

    def as_dict(self) -> dict:
        return {
            "psp_master_switch": self.psp_master_switch(),
            "psp_lockout_switch": self.psp_lockout_switch(),
            "psp_maintenance_switch": self.psp_maintenance_switch(),
            # Echos
            "vmsc_in_control_echo": self.vmsc_in_control_echo(),
            "vmsc1_in_control_echo": self.vmsc1_in_control_echo(),
            "vmsc2_in_control_echo": self.vmsc2_in_control_echo(),
            "vmsc3_in_control_echo": self.vmsc3_in_control_echo(),
            "operational_mode_echo": self.operational_mode_echo(),
            "clear_inflight_bootup_conditions_echo": self.clear_inflight_bootup_conditions_echo(),
            "bat_a_hv_battery_contactor_open_closed_echo": self.bat_a_hv_battery_contactor_open_closed_echo(),
            "bat_b_hv_battery_contactor_open_closed_echo": self.bat_b_hv_battery_contactor_open_closed_echo(),
            "hv_aux_contactor_open_closed_echo": self.hv_aux_contactor_open_closed_echo(),
            "bms_in_control_of_battery_contactors_echo": self.bms_in_control_of_battery_contactors_echo(),
            "enable_cell_balancing_echo": self.enable_cell_balancing_echo(),
            "lock_hv_load_echo": self.lock_hv_load_echo(),
            "lock_lv_load_echo": self.lock_lv_load_echo(),
            # Overview
            "vmsc1_in_control_status": self.vmsc1_in_control_status(),
            "vmsc2_in_control_status": self.vmsc2_in_control_status(),
            "vmsc3_in_control_status": self.vmsc3_in_control_status(),
            "operational_mode_status": self.operational_mode_status(),
            "inflight_bootup": self.inflight_bootup(),
            "bat_a_hv_contactor_status": self.bat_a_hv_battery_contactor_open_closed_status(),
            "bat_b_hv_contactor_status": self.bat_b_hv_battery_contactor_open_closed_status(),
            "hvpmu_positive_charge_contactor": self.hvpmu_positive_charge_contactor(),
            "hvpmu_negative_charge_contactor": self.hvpmu_negative_charge_contactor(),
            "hv_aux_contactor_open_closed_status": self.hv_aux_contactor_open_closed_status(),
            "soft_short_detected": self.hvpmu_soft_short_detected(),
            "hard_short_detected": self.hvpmu_hard_short_detected(),
            "bms_in_control_of_battery_contactors_status": self.bms_in_control_of_battery_contactors_status(),
            "psp_hv_light_status": self.psp_hv_light_status(),
            "psp_lv_light_status": self.psp_lv_light_status(),
            "psp_charge_light_status": self.psp_charge_light_status(),
            "psp_maintenance_light_status": self.psp_maintenance_light_status(),
            "charge_cable_mated": self.charge_cable_mated(),
            "maintenance_bus_mated": self.maintenance_bus_mated(),
            "bat_a_hv_terminal_voltage": self.bat_a_hv_terminal_voltage(),
            "bat_b_hv_terminal_voltage": self.bat_b_hv_terminal_voltage(),
            "bat_a_output_current": self.bat_a_output_current(),
            "bat_b_output_current": self.bat_b_output_current(),
            "bat_a_lvpdu_output_voltage": self.bat_a_lvpdu_output_voltage(),
            "bat_b_lvpdu_output_voltage": self.bat_b_lvpdu_output_voltage(),
            "bat_b_lvpdu_output_current": self.bat_b_lvpdu_output_current(),
            "bat_a_lvpdu_output_current": self.bat_a_lvpdu_output_current(),
            "enable_cell_balancing_status": self.enable_cell_balancing_status(),
            "bat_a_hv_remaining_energy": self.bat_a_hv_remaining_energy(),
            "bat_a_time_to_charged": self.bat_a_time_to_charged(),
            "bat_a_target_state_of_charge": self.bat_a_target_state_of_charge(),
            "bat_a_state_of_charge": self.bat_a_state_of_charge(),
            "bat_a_state_of_health": self.bat_a_state_of_health(),
            "bat_a_charge_capacity": self.bat_a_charge_capacity(),
            "bat_a_battery_charging_status": self.bat_a_battery_charging_status(),
            "bat_b_battery_charging_status": self.bat_b_battery_charging_status(),
            "bat_a_average_cell_temp": self.bat_a_average_cell_temp(),
            "bat_b_average_cell_temp": self.bat_b_average_cell_temp(),
            # sensor errors
            "input_current_mismatch": self.input_current_mismatch(),
            "bat_a_hv_terminal_voltage_sensor_error": self.bat_a_hv_terminal_voltage_sensor_error(),
            "bat_a_hv_current_sensor_error": self.bat_a_hv_current_sensor_error(),
            "bat_a_hv_temperature_sensor_error": self.bat_a_hv_temperature_sensor_error(),
            "bat_a_lvpdu_voltage_sensor_error": self.bat_a_lvpdu_voltage_sensor_error(),
            "bat_a_lvpdu_current_sensor_error": self.bat_a_lvpdu_current_sensor_error(),
            "bat_a_hv_remaining_energy_sensor_error": self.bat_a_hv_remaining_energy_sensor_error(),
            "bat_a_state_of_charge_sensor_error": self.bat_a_state_of_charge_sensor_error(),
            "bat_a_state_of_health_sensor_error": self.bat_a_state_of_health_sensor_error(),
            "bat_a_charge_capacity_sensor_error": self.bat_a_charge_capacity_sensor_error(),
            "bat_a_gas_sensor_error": self.bat_a_gas_sensor_error(),
            "bat_b_hv_terminal_voltage_sensor_error": self.bat_b_hv_terminal_voltage_sensor_error(),
            "bat_b_hv_current_sensor_error": self.bat_b_hv_current_sensor_error(),
            "bat_b_hv_temperature_sensor_error": self.bat_b_hv_temperature_sensor_error(),
            "bat_b_lvpdu_voltage_sensor_error": self.bat_b_lvpdu_voltage_sensor_error(),
            "bat_b_lvpdu_current_sensor_error": self.bat_b_lvpdu_current_sensor_error(),
            "bat_b_hv_remaining_energy_sensor_error": self.bat_b_hv_remaining_energy_sensor_error(),
            "bat_b_state_of_charge_sensor_error": self.bat_b_state_of_charge_sensor_error(),
            "bat_b_state_of_health_sensor_error": self.bat_b_state_of_health_sensor_error(),
            "bat_b_charge_capacity_sensor_error": self.bat_b_charge_capacity_sensor_error(),
            "bat_b_gas_sensor_error": self.bat_b_gas_sensor_error(),
            # charge
            "bat_b_time_to_charged": self.bat_b_time_to_charged(),
            "bat_b_hv_remaining_energy": self.bat_b_hv_remaining_energy(),
            "bat_b_target_state_of_charge": self.bat_b_target_state_of_charge(),
            "bat_b_state_of_health": self.bat_b_state_of_health(),
            "bat_b_charge_capacity": self.bat_b_charge_capacity(),
            # faults
            "bat_b_state_of_charge": self.bat_b_state_of_charge(),
            "bat_a_hv_battery_contactor_mismatch": self.bat_a_hv_battery_contactor_mismatch(),
            "bat_b_hv_battery_contactor_mismatch": self.bat_b_hv_battery_contactor_mismatch(),
            "hv_aux_contactor_mismatch": self.hv_aux_contactor_mismatch(),
            "hvpmu_positive_charge_contactor_mismatch": self.hvpmu_positive_charge_contactor_mismatch(),
            "hvpmu_negative_charge_contactor_mismatch": self.hvpmu_negative_charge_contactor_mismatch(),
            "bat_a_power_isolation_fault": self.bat_a_power_isolation_fault(),
            "bat_b_power_isolation_fault": self.bat_b_power_isolation_fault(),
            "bat_a_return_isolation_fault": self.bat_a_return_isolation_fault(),
            "bat_b_return_isolation_fault": self.bat_b_return_isolation_fault(),
            "pack_to_pack_isolation_fault": self.pack_to_pack_isolation_fault(),
            "thermal_runaway_fault": self.thermal_runaway_fault(),
            "bmic_fault": self.bmic_fault(),
            "dcp_fault": self.dcp_fault(),
            "cbit_rollup": self.cbit_rollup(),
            "hvpmu_output_1_current": self.hvpmu_output_1_current(),
            "hvpmu_output_2_current": self.hvpmu_output_2_current(),
            "hvpmu_aux_output_current": self.hvpmu_aux_output_current(),
            # lv loads
            **{f"lv_{i}_load_feedback": self.lv_load_feedback(i) for i in range(1, 31)},
            **{f"lv_{i}_load_overcurrent": self.lv_load_overcurrent(i) for i in range(1, 31)},
            # metadata
            "bms_part_number": self.bms_part_number(),
            "bms_serial_number": self.bms_serial_number(),
            "firmware_part_number": self.firmware_part_number(),
            # General Telemetry
            "hvpmu_output_voltage": self.hvpmu_output_voltage(),
            # Cell Telemetry Overview
            "bat_a_max_cell_voltage": self.bat_a_max_cell_voltage(),
            "bat_b_max_cell_voltage": self.bat_b_max_cell_voltage(),
            "bat_a_min_cell_voltage": self.bat_a_min_cell_voltage(),
            "bat_b_min_cell_voltage": self.bat_b_min_cell_voltage(),
            "bat_a_max_cell_temperature": self.bat_a_max_cell_temperature(),
            "bat_b_max_cell_temperature": self.bat_b_max_cell_temperature(),
            "bat_a_min_cell_temperature": self.bat_a_min_cell_temperature(),
            "bat_b_min_cell_temperature": self.bat_b_min_cell_temperature(),
        }


def sleep_until_exact(t: datetime.datetime) -> None:
    while datetime.datetime.now() < t:
        pass


def get_channel_data(client: connect_python.Client, channel_num: int, sequence_num: int):
    command = CommandData()
    command.set_bms_id(client.get_value("bmsId"))
    command.set_sequence_num(sequence_num)
    command.set_vmsc_in_control(int(client.get_value("vmsInControl")[-1]) - 1)
    command.set_clear_inflight_bootup_condition(False)
    command.set_vmsc_id(f"VMSC {channel_num}")
    command.set_operational_mode(client.get_value("operationalMode"))
    command.set_bat_a_breaktor_closed(client.get_value(f"ch{channel_num}IsBatAHvBreaktorClosed"))
    command.set_bat_b_breaktor_closed(client.get_value(f"ch{channel_num}IsBatBHvBreaktorClosed"))

    for i in range(1, 31):
        command.set_lv_load(i, client.get_value(f"ch{channel_num}_lv_load_{i}"))

    return command.get_data()


def send_data(client: connect_python.Client, channel: Channel, channel_num: int, sequence_num: int):
    # Get the data for the channel
    ch_data = get_channel_data(client, channel_num, sequence_num)

    # Write the data to the FPGA
    channel.data_register().write(ch_data)

    client.set_value(f"ch{channel_num}Payload", str(ch_data[0:30]))

    # Trigger the send signal and wait briefly for the registers to update
    channel.send_register().write(True)
    time.sleep(0.00005)
    channel.send_register().write(False)


def receive_data(client: connect_python.Client, channel: Channel, channel_num: int):
    try:
        response = _receive_data(client, channel, channel_num)
    except Exception as e:
        print(f"Error receiving data from channel {channel_num}: {e}", flush=True)
        return None

    return response


def _receive_data(client: connect_python.Client, channel: Channel, channel_num: int):
    # Read the received data
    frame = channel.frame_register().read()
    frames_received = channel.frames_received_register().read()
    bytes_received = channel.bytes_received_register().read()

    print(f"Channel {channel_num} frames received: {frames_received}", flush=True)
    response = ResponseData(frame[:bytes_received])

    client.set_value(f"ch{channel_num}Response", str(frame[:bytes_received]))
    client.set_value(f"ch{channel_num}OperationalModeEcho", response.operational_mode_echo())

    # TODO: Record responses in a CSV file
    return response


def plot_response(client: connect_python.Client, response: ResponseData):
    for stream_name, stream_func in response.as_dict().items():
        if stream_name not in [
            "timestamp",
            "operational_mode_echo",
            "bms_part_number",
            "bms_serial_number",
            "firmware_part_number",
            "operational_mode_status",
        ]:
            client.stream(stream_name, t, float(stream_func))

    output_row = [
        response.operational_mode_echo(),
        response.operational_mode_status(),
    ]
    client.set_output(
        {
            "columns": ["operational_mode_echo", "operational_mode_status"],
            "data": [[str(value)] for value in output_row],
        }
    )


t = 0


def run(client: connect_python.Client, session: nifpga.Session):
    print("Connected to FPGA.", flush=True)

    logs_folder = "./logs"
    log_file_ch1 = log_file_path(logs_folder, "fcb_ch1")
    log_file_ch2 = log_file_path(logs_folder, "fcb_ch2")

    for stream_name in ResponseData().as_dict():
        client.clear_stream(stream_name)

    ch1 = Channel(session, 1)
    ch2 = Channel(session, 2)

    # Set the size (in bytes) of the data being transmitted
    ch1.size_register().write(30)
    ch2.size_register().write(30)

    sequence_num = 0
    test_start = datetime.datetime.now()
    while True:
        cycle_start = datetime.datetime.now()
        send_data(client, ch1, 1, sequence_num)
        send_data(client, ch2, 2, sequence_num)

        global t
        t = cycle_start.timestamp() - test_start.timestamp()

        ch1_response = receive_data(client, ch1, 1)
        ch2_response = receive_data(client, ch2, 2)
        response_ts = time.time_ns()

        if ch1_response is not None:
            client.set_value("hvpmu_positive_charge_contactor", ch1_response.hvpmu_positive_charge_contactor())
            client.set_value("hvpmu_negative_charge_contactor", ch1_response.hvpmu_negative_charge_contactor())

            plot_response(client, ch1_response)
            dict = ch1_response.as_dict()
            dict["timestamp"] = response_ts
            write_data(
                path=log_file_ch1,
                client=client,
                df=df_from_scalar_dict(dict),
                streaming_enabled=get_streaming_enabled(client),
                stream_id="fcb_ch1",
            )

        if ch2_response is not None:
            dict = ch2_response.as_dict()
            dict["timestamp"] = response_ts
            write_data(
                path=log_file_ch2,
                client=client,
                df=df_from_scalar_dict(dict),
                streaming_enabled=get_streaming_enabled(client),
                stream_id="fcb_ch2",
            )

        # Increment the sequence number
        if sequence_num < 65536:
            sequence_num += 1
        else:
            sequence_num = 0

        # Sleep to maintain a 100Hz cycle
        sleep_until_exact(cycle_start + datetime.timedelta(milliseconds=10))


@connect_python.main
def main(client: connect_python.Client):
    bitfile_path = "./bitfiles/FCB-2Chan-9053.lvbitx"
    resource = client.get_value("resource")

    client.set_value("dataset_rid", config.dataset_rid)
    try:
        # Flight control bus comms via cRIO
        with nifpga.Session(bitfile_path, resource, reset_if_last_session_on_exit=True) as session:
            run(client, session)
    finally:
        pass


if __name__ == "__main__":
    main()
