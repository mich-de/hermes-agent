"""TTS wrapper: testo → WAV 24kHz con voce KITT. Usa Pocket TTS da C:\\npu\\kitt."""
import sys
import os
import wave

import numpy as np

sys.path.insert(0, r"C:\npu\kitt")
from voice_kitt import Voice, RATE

voice = Voice()

text = sys.argv[1]
out_path = sys.argv[2]

audio = voice.synthesize(text)
pcm = (np.clip(audio, -1, 1) * 32767).astype(np.int16)

with wave.open(out_path, "wb") as w:
    w.setnchannels(1)
    w.setsampwidth(2)
    w.setframerate(RATE)
    w.writeframes(pcm.tobytes())
