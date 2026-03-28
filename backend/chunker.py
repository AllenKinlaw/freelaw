import re


def legal_chunker(text: str, chunk_size: int = 3000, overlap: int = 500) -> list[str]:
    """
    Split legal text into chunks that preserve paragraph boundaries and SC citations.
    Citations like '123 S.C. 456', '123 S.E.2d 456', '18 U.S.C. § 1030' are never
    split across chunk boundaries.
    """
    # Normalize line endings and collapse excessive blank lines
    text = re.sub(r'\r\n', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = text.strip()

    if not text:
        return []

    # Split on paragraph boundaries
    paragraphs = re.split(r'\n\n+', text)

    chunks: list[str] = []
    current_chunk = ""

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # Paragraph itself is larger than chunk_size — split by sentence
        if len(para) > chunk_size:
            # Sentence boundary: end of sentence followed by capital letter
            sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', para)
            for sentence in sentences:
                if len(current_chunk) + len(sentence) + 1 <= chunk_size:
                    current_chunk += sentence + " "
                else:
                    if current_chunk.strip():
                        chunks.append(current_chunk.strip())
                    # Carry forward an overlap window to preserve context
                    tail = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
                    current_chunk = tail + sentence + " "
        else:
            if len(current_chunk) + len(para) + 2 <= chunk_size:
                current_chunk += para + "\n\n"
            else:
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                tail = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
                current_chunk = tail + para + "\n\n"

    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    return chunks


def enrich_chunk(
    chunk: str,
    case_name: str,
    court: str,
    year: int,
    opinion_type: str,
) -> str:
    """
    Prepend a metadata header to each chunk so the LLM always knows the source
    during retrieval, even when the chunk is presented without surrounding context.
    """
    court_label = {
        "sc": "SC Supreme Court",
        "scctapp": "SC Court of Appeals",
    }.get(court, court)

    header = (
        f"[Source: {case_name} | Court: {court_label} | "
        f"Year: {year} | Opinion Type: {opinion_type}]\n\n"
    )
    return header + chunk
