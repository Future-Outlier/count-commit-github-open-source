FROM ghcr.io/opensource4you/texlive-ja

RUN apt update && apt upgrade -y && apt install -y wget curl vim
RUN curl -LO https://github.com/quarto-dev/quarto-cli/releases/download/v1.8.27/quarto-1.8.27-linux-amd64.deb \
    && apt install ./quarto-1.8.27-linux-amd64.deb -y \
    && rm quarto-1.8.27-linux-amd64.deb

RUN quarto install tinytex
RUN mkdir /tmp/quarto-resume-template
WORKDIR /tmp/quarto-resume-template
RUN quarto use template machichima/quarto-resume-template --no-prompt
RUN quarto render resume.qmd --to pdf