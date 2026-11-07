"""STT wrapper: WAV 16kHz mono → testo. Usa sherpa-onnx Whisper small da C:\\npu."""
import os
import sys

import sherpa_onnx

MODEL = r"C:\NPU\models\whisper-small"

recognizer = sherpa_onnx.OfflineRecognizer.from_whisper(
    encoder=os.path.join(MODEL, "small-encoder.int8.onnx"),
    decoder=os.path.join(MODEL, "small-decoder.int8.onnx"),
    tokens=os.path.join(MODEL, "small-tokens.txt"),
    language="it",
    task="transcribe",
    num_threads=4,
)

wav_path = sys.argv[1]
import wave
import numpy as np

with wave.open(wav_path, "rb") as w:
    frames = w.readframes(w.getnframes())
    audio = np.frombuffer(frames, dtype=np.int16).astype(np.float32) / 32768.0

stream = recognizer.create_stream()
stream.accept_waveform(16000, audio)
recognizer.decode_stream(stream)
print(stream.result.text.strip())
